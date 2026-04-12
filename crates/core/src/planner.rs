use crate::models::{Action, ActionType, DiffEntry, DiffStatus, MismatchStrategy};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

pub fn plan_actions(
    diffs: &[DiffEntry],
    strategy: MismatchStrategy,
    overrides: &HashMap<PathBuf, ActionType>,
    copied_recently: &BTreeSet<PathBuf>,
    force_recopy: &BTreeSet<PathBuf>,
) -> Vec<Action> {
    let mut actions = Vec::new();
    for diff in diffs {
        if let Some(&override_action) = overrides.get(&diff.path_rel) {
            actions.push(Action { path_rel: diff.path_rel.clone(), action_type: override_action, reason: "override".to_string() });
            continue;
        }
        if copied_recently.contains(&diff.path_rel) && !force_recopy.contains(&diff.path_rel) {
            continue;
        }
        match diff.status {
            DiffStatus::MissingLeft => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyRightToLeft, reason: "missing-left".to_string() }),
            DiffStatus::MissingRight => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyLeftToRight, reason: "missing-right".to_string() }),
            DiffStatus::Mismatch => match strategy {
                MismatchStrategy::Skip => {}
                MismatchStrategy::PreferLeft => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyLeftToRight, reason: "mismatch-prefer-left".to_string() }),
                MismatchStrategy::PreferRight => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyRightToLeft, reason: "mismatch-prefer-right".to_string() }),
                MismatchStrategy::NewerMtime => {
                    let lm = diff.left.as_ref().map(|m| m.mtime).unwrap_or(0);
                    let rm = diff.right.as_ref().map(|m| m.mtime).unwrap_or(0);
                    if lm >= rm {
                        actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyLeftToRight, reason: "mismatch-newer-left".to_string() });
                    } else {
                        actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyRightToLeft, reason: "mismatch-newer-right".to_string() });
                    }
                }
            },
            DiffStatus::Conflict | DiffStatus::Same => {}
        }
    }
    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::FileMeta;
    use std::collections::BTreeSet;

    #[test]
    fn plan_actions_uses_strategy() {
        let diffs = vec![DiffEntry {
            path_rel: PathBuf::from("x.txt"),
            left: Some(FileMeta { size: 1, mtime: 5, hash: None, is_symlink: false, link_target: None }),
            right: Some(FileMeta { size: 1, mtime: 1, hash: None, is_symlink: false, link_target: None }),
            status: DiffStatus::Mismatch,
        }];
        let actions = plan_actions(&diffs, MismatchStrategy::NewerMtime, &HashMap::new(), &BTreeSet::new(), &BTreeSet::new());
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0].action_type, ActionType::CopyLeftToRight));
    }

    #[test]
    fn plan_actions_override_wins_over_copied_recently() {
        let path = PathBuf::from("file.txt");
        let diffs = vec![DiffEntry {
            path_rel: path.clone(),
            left: None,
            right: Some(FileMeta { size: 1, mtime: 1, hash: None, is_symlink: false, link_target: None }),
            status: DiffStatus::MissingLeft,
        }];
        let mut overrides = HashMap::new();
        overrides.insert(path.clone(), ActionType::DeleteRight);
        let mut copied_recently = BTreeSet::new();
        copied_recently.insert(path.clone());
        let actions = plan_actions(&diffs, MismatchStrategy::Skip, &overrides, &copied_recently, &BTreeSet::new());
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0].action_type, ActionType::DeleteRight));
    }
}
