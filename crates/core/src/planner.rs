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
            // For exact strategies, still emit delete actions even for recently-copied files
            let is_exact_delete = matches!(
                (strategy, &diff.status),
                (MismatchStrategy::ExactLeftToRight, DiffStatus::MissingLeft) |
                (MismatchStrategy::ExactRightToLeft, DiffStatus::MissingRight)
            );
            if !is_exact_delete { continue; }
        }
        match diff.status {
            DiffStatus::MissingLeft => match strategy {
                MismatchStrategy::ExactLeftToRight => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::DeleteRight, reason: "exact-l2r-delete-right".to_string() }),
                _ => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyRightToLeft, reason: "missing-left".to_string() }),
            },
            DiffStatus::MissingRight => match strategy {
                MismatchStrategy::ExactRightToLeft => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::DeleteLeft, reason: "exact-r2l-delete-left".to_string() }),
                _ => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyLeftToRight, reason: "missing-right".to_string() }),
            },
            DiffStatus::Mismatch => match strategy {
                MismatchStrategy::Skip => {}
                MismatchStrategy::PreferLeft | MismatchStrategy::ExactLeftToRight => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyLeftToRight, reason: "mismatch-prefer-left".to_string() }),
                MismatchStrategy::PreferRight | MismatchStrategy::ExactRightToLeft => actions.push(Action { path_rel: diff.path_rel.clone(), action_type: ActionType::CopyRightToLeft, reason: "mismatch-prefer-right".to_string() }),
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

    #[test]
    fn exact_left_to_right_deletes_extras_on_right_and_copies_missing() {
        // missing-right → copy L→R; missing-left → delete right; mismatch → copy L→R
        let diffs = vec![
            DiffEntry { path_rel: PathBuf::from("only_left.txt"), left: Some(FileMeta { size: 1, mtime: 1, hash: None, is_symlink: false, link_target: None }), right: None, status: DiffStatus::MissingRight },
            DiffEntry { path_rel: PathBuf::from("only_right.txt"), left: None, right: Some(FileMeta { size: 1, mtime: 1, hash: None, is_symlink: false, link_target: None }), status: DiffStatus::MissingLeft },
            DiffEntry { path_rel: PathBuf::from("mismatch.txt"), left: Some(FileMeta { size: 1, mtime: 2, hash: None, is_symlink: false, link_target: None }), right: Some(FileMeta { size: 2, mtime: 1, hash: None, is_symlink: false, link_target: None }), status: DiffStatus::Mismatch },
        ];
        let actions = plan_actions(&diffs, MismatchStrategy::ExactLeftToRight, &HashMap::new(), &BTreeSet::new(), &BTreeSet::new());
        assert_eq!(actions.len(), 3);
        let by_path: HashMap<_, _> = actions.iter().map(|a| (a.path_rel.as_path(), &a.action_type)).collect();
        assert!(matches!(by_path[std::path::Path::new("only_left.txt")], ActionType::CopyLeftToRight));
        assert!(matches!(by_path[std::path::Path::new("only_right.txt")], ActionType::DeleteRight));
        assert!(matches!(by_path[std::path::Path::new("mismatch.txt")], ActionType::CopyLeftToRight));
    }

    #[test]
    fn exact_right_to_left_deletes_extras_on_left_and_copies_missing() {
        // missing-left → copy R→L; missing-right → delete left; mismatch → copy R→L
        let diffs = vec![
            DiffEntry { path_rel: PathBuf::from("only_right.txt"), left: None, right: Some(FileMeta { size: 1, mtime: 1, hash: None, is_symlink: false, link_target: None }), status: DiffStatus::MissingLeft },
            DiffEntry { path_rel: PathBuf::from("only_left.txt"), left: Some(FileMeta { size: 1, mtime: 1, hash: None, is_symlink: false, link_target: None }), right: None, status: DiffStatus::MissingRight },
            DiffEntry { path_rel: PathBuf::from("mismatch.txt"), left: Some(FileMeta { size: 1, mtime: 1, hash: None, is_symlink: false, link_target: None }), right: Some(FileMeta { size: 2, mtime: 2, hash: None, is_symlink: false, link_target: None }), status: DiffStatus::Mismatch },
        ];
        let actions = plan_actions(&diffs, MismatchStrategy::ExactRightToLeft, &HashMap::new(), &BTreeSet::new(), &BTreeSet::new());
        assert_eq!(actions.len(), 3);
        let by_path: HashMap<_, _> = actions.iter().map(|a| (a.path_rel.as_path(), &a.action_type)).collect();
        assert!(matches!(by_path[std::path::Path::new("only_right.txt")], ActionType::CopyRightToLeft));
        assert!(matches!(by_path[std::path::Path::new("only_left.txt")], ActionType::DeleteLeft));
        assert!(matches!(by_path[std::path::Path::new("mismatch.txt")], ActionType::CopyRightToLeft));
    }

    #[test]
    fn exact_delete_not_skipped_by_copied_recently() {
        // Regression: a file in copied_recently that now needs deleting must not be skipped
        let path = PathBuf::from("was_copied_now_extra.txt");
        let diffs = vec![DiffEntry {
            path_rel: path.clone(),
            left: None,
            right: Some(FileMeta { size: 1, mtime: 1, hash: None, is_symlink: false, link_target: None }),
            status: DiffStatus::MissingLeft,
        }];
        let mut copied_recently = BTreeSet::new();
        copied_recently.insert(path.clone());
        // ExactLeftToRight: MissingLeft → DeleteRight, must fire even if in copied_recently
        let actions = plan_actions(&diffs, MismatchStrategy::ExactLeftToRight, &HashMap::new(), &copied_recently, &BTreeSet::new());
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0].action_type, ActionType::DeleteRight));
    }
}
