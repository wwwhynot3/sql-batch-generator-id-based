use anyhow::{Result, anyhow};

#[derive(Debug, Clone, Copy)]
pub struct IdBatchRange {
    pub start_id: i128,
    pub end_id: i128,
}

#[derive(Debug)]
pub struct IdBatchSlicer {
    start_id: i128,
    end_id: i128,
    batch_size: usize,
}

impl IdBatchSlicer {
    pub fn new(start_id: i128, end_id: i128, batch_size: usize) -> Result<Self> {
        if start_id > end_id {
            return Err(anyhow!("End ID must be greater than or equal to Start ID"));
        }
        if batch_size == 0 {
            return Err(anyhow!("Batch size must be greater than 0"));
        }

        Ok(Self {
            start_id,
            end_id,
            batch_size,
        })
    }

    pub fn iter_ranges(&self) -> impl Iterator<Item = IdBatchRange> + '_ {
        let batch_size_as_i128 = self.batch_size as i128;
        (self.start_id..=self.end_id)
            .step_by(self.batch_size)
            .map(move |current_start| IdBatchRange {
                start_id: current_start,
                end_id: (current_start + batch_size_as_i128 - 1).min(self.end_id),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::IdBatchSlicer;

    #[test]
    fn iter_ranges_caps_last_batch() {
        let slicer = IdBatchSlicer::new(1, 105, 50).expect("slicer should be created");
        let ranges = slicer
            .iter_ranges()
            .map(|range| (range.start_id, range.end_id))
            .collect::<Vec<_>>();

        assert_eq!(ranges, vec![(1, 50), (51, 100), (101, 105)]);
    }
}
