#[cfg(test)]
mod tests {
    use super::super::*;
    use std::path::PathBuf;
    use crossterm::event::{KeyCode, KeyModifiers};

    // We need to bring in the necessary structs and enums. 
    // Since they are in main.rs, we might need to make them public or move them to a lib.rs.
    // For a quick repro, I'll check if they are already available to tests in main.rs.

    #[test]
    fn test_space_selection_and_delete_with_filter() {
        let mut state = AppState::new();
        
        // Mock diffs
        state.diffs = vec![
            DiffEntry {
                path_rel: PathBuf::from("file1"),
                status: DiffStatus::MissingLeft,
                left: None,
                right: None,
            },
            DiffEntry {
                path_rel: PathBuf::from("file2"),
                status: DiffStatus::MissingRight,
                left: None,
                right: None,
            },
            DiffEntry {
                path_rel: PathBuf::from("file3"),
                status: DiffStatus::MissingLeft,
                left: None,
                right: None,
            },
        ];

        // Apply filter: MissingLeft
        state.filter = Filter::MissingLeft;
        recompute_filtered_indices(&mut state);
        // filtered_indices should be [0, 2]

        assert_eq!(state.filtered_indices, vec![0, 2]);

        // Select the second item in the filtered list ("file3", which is index 2 in diffs)
        state.selected.select(Some(1)); 

        // Simulate Space key
        handle_review_input(&mut state, KeyCode::Char(' '), KeyModifiers::empty());

        // Verify that index 2 is selected
        assert!(state.selected_items.contains(&2), "Index 2 should be in selected_items");
        assert!(!state.selected_items.contains(&1), "Index 1 should NOT be in selected_items");

        // Simulate Delete key
        handle_review_input(&mut state, KeyCode::Delete, KeyModifiers::empty());

        // apply_delete_override is called. It uses selected_items if not empty.
        // It should use indices in selected_items to apply overrides.
        
        assert!(state.action_overrides.contains_key(&PathBuf::from("file3")), "file3 should have an override");
        assert_eq!(state.action_overrides.get(&PathBuf::from("file3")), Some(&ActionType::DeleteRight));
        
        // Ensure file1 (index 0, also filtered but not selected) does NOT have an override
        assert!(!state.action_overrides.contains_key(&PathBuf::from("file1")), "file1 should NOT have an override");
    }

    #[test]
    fn test_delete_without_space_selection_with_filter() {
        let mut state = AppState::new();
        
        state.diffs = vec![
            DiffEntry {
                path_rel: PathBuf::from("file1"),
                status: DiffStatus::MissingLeft,
                left: None,
                right: None,
            },
            DiffEntry {
                path_rel: PathBuf::from("file2"),
                status: DiffStatus::MissingRight,
                left: None,
                right: None,
            },
        ];

        // Apply filter: MissingRight
        state.filter = Filter::MissingRight;
        recompute_filtered_indices(&mut state);
        // filtered_indices should be [1]

        assert_eq!(state.filtered_indices, vec![1]);

        // Select the only item in filtered list
        state.selected.select(Some(0));

        // Simulate Delete key (without Space selection)
        handle_review_input(&mut state, KeyCode::Delete, KeyModifiers::empty());

        // apply_delete_override should use the current selection from filtered_indices
        // BUT wait, looking at apply_delete_override:
        // vec![state.selected.selected().unwrap_or(0)]
        // It uses the local index as a global index! This is likely a bug.

        assert!(state.action_overrides.contains_key(&PathBuf::from("file2")), "file2 should have an override");
        assert_eq!(state.action_overrides.get(&PathBuf::from("file2")), Some(&ActionType::DeleteLeft));
    }
}
