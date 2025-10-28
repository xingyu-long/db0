// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::HashMap, usize};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // we will start calculation after we reached the num_tiers.
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let mut size = 0;
        for id in 0..(_snapshot.levels.len() - 1) {
            size += _snapshot.levels[id].1.len();
        }
        // case 1: Triggered by Space Amplification Ratio
        let space_amp_ratio =
            size as f64 / (_snapshot.levels.last().unwrap().1.len()) as f64 * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // case 2: Triggered by Size Ratio
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut prev_size = _snapshot.levels[0].1.len() as f64;
        for i in 1.._snapshot.levels.len() {
            let (tier_id, files) = &_snapshot.levels[i];
            let current_size = files.len() as f64;
            let size_ratio = current_size / prev_size;
            if size_ratio > size_ratio_trigger && i >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size ratio: {} > {}",
                    size_ratio, size_ratio_trigger
                );
                return Some(TieredCompactionTask {
                    tiers: (&_snapshot.levels[0..i]).to_vec(),
                    // NOTE: for tiered, we always looking for previous levels, as i will be
                    // end as levels.len() - 1, so we would never include bottom tier.
                    bottom_tier_included: false,
                });
            }
            prev_size += current_size;
        }

        // case 3: reduce sorted run
        // we will do a major compaction that merges SST files from the first up
        // to max_merge_tiers tiers into one tier to reduce the number of tiers.
        let max_merge_iters = self
            .options
            .max_merge_width
            .unwrap_or(usize::MAX)
            .min(_snapshot.levels.len());
        println!(
            "compaction triggered by max merge width: {}",
            max_merge_iters
        );
        Some(TieredCompactionTask {
            tiers: (&_snapshot.levels[0..max_merge_iters]).to_vec(),
            bottom_tier_included: max_merge_iters >= _snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();

        // remove _task's levels from snapshot
        let mut new_tier_added = false;
        let mut to_be_removed = Vec::new();
        let mut new_levels = Vec::new();
        let mut tier_to_remove = _task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();

        for (tier_id, files) in &snapshot.levels {
            // iterate all levels and remove items based on tier_to_remove;
            if let Some(ffiles) = tier_to_remove.remove(tier_id) {
                assert_eq!(files, ffiles);
                to_be_removed.extend(ffiles);
            } else {
                new_levels.push((*tier_id, files.clone()));
            }

            // _output might be just to compact some levels and append it into the end
            if tier_to_remove.is_empty() && !new_tier_added {
                new_tier_added = true;
                new_levels.push((_output[0], _output.to_vec()));
            }
        }

        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found??");
        }

        snapshot.levels = new_levels;
        return (snapshot, to_be_removed);
    }
}
