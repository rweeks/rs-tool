use std::cmp::{max, min};
use std::collections::HashMap;
use std::hash::Hash;
use fastrand::Rng;

#[derive(Debug)]
pub struct Reservoir<T> {
    capacity: usize,
    pool: Vec<T>,
    pool_full: bool,
    rng: Rng,
    num_adds: u32,
}

impl<T> Reservoir<T> {
    pub fn new(capacity: usize) -> Reservoir<T> {
        Reservoir {
            capacity,
            pool: Vec::with_capacity(capacity),
            pool_full: false,
            rng: Rng::new(),
            num_adds: 0,
        }
    }

    pub fn add(&mut self, item: T) {
        self.num_adds += 1;
        if !self.pool_full {
            self.pool.push(item);
            if self.pool.len() == self.capacity {
                self.pool_full = true;
            }
        } else {
            let j = self.rng.u32(0..self.num_adds);
            if j < self.capacity as u32 {
                self.pool[j as usize] = item;
            }
        }
    }
}

impl <T:Clone> Reservoir<T> {
    pub fn merge(r1: &Reservoir<T>, r2: &Reservoir<T>) -> Reservoir<T> {
        let r1_threshold = r1.num_adds as f32 / (r1.num_adds + r2.num_adds) as f32;
        let r2_threshold = r2.num_adds as f32 / (r1.num_adds + r2.num_adds) as f32;
        let pool_capacity = max(r1.capacity, r2.capacity);
        let mut pool: Vec<T> = Vec::with_capacity(pool_capacity);
        let mut rng = Rng::new();
        for r1_item in &r1.pool {
            if rng.f32() < r1_threshold {
                if pool.len() < pool_capacity {
                    pool.push(r1_item.clone());
                } else {
                    let index_to_evict = rng.u32(0..pool_capacity as u32);
                    pool[index_to_evict as usize] = r1_item.clone();
                }
            }
        }
        for r2_item in &r2.pool {
            if rng.f32() < r2_threshold {
                if pool.len() < pool_capacity {
                    pool.push(r2_item.clone());
                } else {
                    let index_to_evict = rng.u32(0..pool_capacity as u32);
                    pool[index_to_evict as usize] = r2_item.clone();
                }
            }
        }
        Reservoir {
            capacity: pool_capacity,
            pool_full: pool.len() == pool_capacity,
            pool,
            rng,
            num_adds: r1.num_adds + r2.num_adds,
        }
    }
}

impl <T:Eq + Hash> Reservoir<T> {
    pub fn to_histogram(&self) -> HashMap<&T, f32> {
        let mut counts : HashMap<&T, i32> = HashMap::new();
        for item in &self.pool {
            let count = counts.entry(item).or_insert(0);
            *count += 1;
        }
        if self.capacity == 0 {
            HashMap::new()
        } else {
            let effective_size = min(self.pool.len() as u32, self.num_adds) as f32;
            counts.iter().map(|(k, v)| (*k, *v as f32 / effective_size) ).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let r: Reservoir<String> = Reservoir::new(15);
        assert_eq!(HashMap::new(), r.to_histogram());
    }

    #[test]
    fn test_single_entry() {
        let mut r: Reservoir<&str> = Reservoir::new(15);
        r.add("hello");
        let mut v = HashMap::new();
        v.insert(&"hello", 1.0f32);
        assert_eq!(v, r.to_histogram());
    }

    #[test]
    fn test_single_entry_overflow() {
        let mut r: Reservoir<&str> = Reservoir::new(2);
        r.add("hello");
        r.add("hello");
        r.add("hello");
        let mut v = HashMap::new();
        v.insert(&"hello", 1.0f32);
        assert_eq!(v, r.to_histogram());
    }

    #[test]
    fn test_many_entries_2_types() {
        let mut r = Reservoir::new(100);
        for _ in 0..5000 {
            r.add("hello");
        }
        for _ in 0..5000 {
            r.add("world");
        }
        let h = r.to_histogram();
        let hello_freq = h.get(&"hello").unwrap();
        let world_freq = h.get(&"world").unwrap();
        assert!((1.0f32 - (hello_freq + world_freq)).abs() < 0.001f32);
        assert!((hello_freq - world_freq).abs() < 0.1f32);
    }

    #[test]
    fn test_merge() {
        let mut r1 = Reservoir::new(1000);
        let mut r2 = Reservoir::new(1000);
        for _ in 0..1000 {
            r1.add("hello");
            r2.add("world");
        }
        let r3 = Reservoir::merge(&r1, &r2);
        assert_eq!(1000, r3.capacity);
        let h = r3.to_histogram();
        let hello_freq = h.get(&"hello").unwrap();
        let world_freq = h.get(&"world").unwrap();
        let dist_result = (1.0f32 - (hello_freq + world_freq)).abs();
        assert!(dist_result < 0.001f32,
                "hello_freq == {hello_freq} world_freq == {world_freq} result == {dist_result}");
        assert!((hello_freq - world_freq).abs() < 0.1f32);
    }
}