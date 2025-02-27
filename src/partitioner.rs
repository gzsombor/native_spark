use super::*;
use fasthash::MetroHasher;
//use objekt;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

// Partitioner trait for creating Rdd partitions
//pub trait Partitioner: objekt::Clone + Send + Sync  {
//pub trait PartitionerBase: Serialize + Deserialize {
//    fn equals(&self, other: &dyn Any) -> bool;
//    fn get_num_of_partitions(&self) -> usize;
//}

pub trait Partitioner: Send + Sync + objekt::Clone + Serialize + Deserialize {
    fn equals(&self, other: &dyn Any) -> bool;
    fn get_num_of_partitions(&self) -> usize;
    fn get_partition(&self, key: &dyn Any) -> usize;
}
objekt::clone_trait_object!(Partitioner);

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s: MetroHasher = Default::default();
    t.hash(&mut s);
    s.finish()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashPartitioner<K: Data + Hash + Eq> {
    partitions: usize,
    _marker: PhantomData<K>,
}

// Hash partitioner implementing naive hash function.
impl<K: Data + Hash + Eq> HashPartitioner<K> {
    pub fn new(partitions: usize) -> Self {
        HashPartitioner {
            partitions,
            _marker: PhantomData,
        }
    }
}

//impl<K: Data + Hash + Eq> PartitionerBase for HashPartitioner<K> {
//    fn equals(&self, other: &dyn Any) -> bool {
//        if let Some(hp) = other.downcast_ref::<HashPartitioner<K>>() {
//            self.partitions == hp.partitions
//        } else {
//            false
//        }
//    }
//    fn get_num_of_partitions(&self) -> usize {
//        self.partitions
//    }
//}

impl<K: Data + Hash + Eq> Partitioner for HashPartitioner<K> {
    fn equals(&self, other: &dyn Any) -> bool {
        if let Some(hp) = other.downcast_ref::<HashPartitioner<K>>() {
            self.partitions == hp.partitions
        } else {
            false
        }
    }
    fn get_num_of_partitions(&self) -> usize {
        self.partitions
    }
    fn get_partition(&self, key: &dyn Any) -> usize {
        let key = key.downcast_ref::<K>().unwrap();
        hash(key) as usize % self.partitions
        //        if modulus < 0 {
        //            modulus + self.partitions
        //        } else {
        //            modulus
        //        }
    }
}

//impl PartialEq<Any> for HashPartitioner {
//    fn eq(&self, other: &Any) -> bool {
//        if let Some(hp) = other.downcast_ref::<HashPartitioner>() {
//            self.partitions == hp.partitions
//        } else {
//            false
//        }
//    }
//}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hash_partition() {
        //let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 8, 8];
        let data = vec![1, 2];
        let num_partition = 3;
        let hash_partitioner = HashPartitioner::<i32>::new(num_partition);
        for i in &data {
            println!("value: {:?}-hash: {:?}", i, hash(i));
            println!(
                "value: {:?}-index: {:?}",
                i,
                hash_partitioner.get_partition(i)
            );
        }
        let mut partition = vec![Vec::new(); num_partition];
        for i in &data {
            let index = hash_partitioner.get_partition(i);
            partition[index].push(i)
        }
        assert_eq!(partition.len(), 3)
    }

    #[test]
    fn hash_partitioner_eq() {
        let p1 = HashPartitioner::<i32>::new(1);
        let p2_1 = HashPartitioner::<i32>::new(2);
        let p2_2 = HashPartitioner::<i32>::new(2);
        assert!(p1.equals(&p1));
        assert!(p1.clone().equals(&p1));
        assert!(p2_1.equals(&p2_1));
        assert!(p2_1.equals(&p2_2));
        assert!(p2_2.equals(&p2_1));
        assert!(!p1.equals(&p2_1));
        assert!(!p1.equals(&p2_2));
        let mut p1 = Some(p1);
        assert!(p1.clone().map(|p| (&p).equals(&p1.clone().unwrap())) == Some(true));
        assert!(p1.clone().map(|p| p.equals(&p2_1.clone())) == Some(false));
        assert!(p1.clone().map(|p| p.equals(&p2_2.clone())) == Some(false));
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) != None);
        assert!(
            p1.clone()
                .map_or(false, |p| (&p).equals(&p1.clone().unwrap()))
        );
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_1.clone())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_2.clone())));
        p1 = None;
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) == None);
        assert!(p1.clone().map(|p| p.equals(&p2_1.clone())) == None);
        assert!(p1.clone().map(|p| p.equals(&p2_2.clone())) == None);
        assert!(
            !p1.clone()
                .map_or(false, |p| (&p).equals(&p1.clone().unwrap()))
        );
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_1.clone())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_2.clone())));
    }
}
