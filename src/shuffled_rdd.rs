use super::*;
//use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
//use std::marker::PhantomData;
use std::sync::Arc;
//use std::time;
//use std::time::Duration;
use std::time::SystemTime;
//use std::any::Any;

#[derive(Clone, Serialize, Deserialize)]
struct ShuffledRddSplit {
    index: usize,
}
impl ShuffledRddSplit {
    fn new(index: usize) -> Self {
        ShuffledRddSplit { index }
    }
}

impl Split for ShuffledRddSplit {
    fn get_index(&self) -> usize {
        self.index
    }
}

#[derive(Serialize, Deserialize)]
pub struct ShuffledRdd<K: Data + Eq + Hash, V: Data, C: Data, RT: 'static>
where
    RT: Rdd<(K, V)>,
{
    #[serde(with = "serde_traitobject")]
    parent: Arc<RT>,
    #[serde(with = "serde_traitobject")]
    aggregator: Arc<Aggregator<K, V, C>>,
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    part: Box<dyn Partitioner>,
    shuffle_id: usize,
}

impl<K: Data + Eq + Hash, V: Data, C: Data, RT: 'static> Clone for ShuffledRdd<K, V, C, RT>
where
    RT: Rdd<(K, V)>,
{
    fn clone(&self) -> Self {
        ShuffledRdd {
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            vals: self.vals.clone(),
            part: self.part.clone(),
            shuffle_id: self.shuffle_id,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data, RT: 'static> ShuffledRdd<K, V, C, RT>
where
    RT: Rdd<(K, V)>,
{
    pub fn new(
        parent: Arc<RT>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let mut vals = RddVals::new(parent.get_context());
        let shuffle_id = vals.context.new_shuffle_id();

        vals.dependencies
            .push(Dependency::ShuffleDependency(Arc::new(
                ShuffleDependency::new(
                    shuffle_id,
                    false,
                    parent.get_rdd(),
                    aggregator.clone(),
                    part.clone(),
                ),
            )));
        let vals = Arc::new(vals);
        ShuffledRdd {
            parent,
            aggregator,
            vals,
            part,
            shuffle_id,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data, RT: 'static> RddBase for ShuffledRdd<K, V, C, RT>
where
    RT: Rdd<(K, V)>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }
    fn get_context(&self) -> Context {
        self.vals.context.clone()
    }
    fn get_dependencies(&self) -> &[Dependency] {
        &self.vals.dependencies
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.part.get_num_of_partitions())
            .map(|x| Box::new(ShuffledRddSplit::new(x)) as Box<dyn Split>)
            .collect()
    }
    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        Some(self.part.clone())
    }
    fn iterator_any(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any shuffledrdd",);
        Box::new(
            self.iterator(split)
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        )
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside cogroup iterator_any shuffledrdd",);
        Box::new(
            self.iterator(split)
                .map(|(k, v)| Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>),
        )
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data, RT: 'static> Rdd<(K, C)> for ShuffledRdd<K, V, C, RT>
where
    RT: Rdd<(K, V)>,
{
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
    //    fn partitioner<P1: Partitioner + Clone>(&self) -> Option<P1> {
    //        Some(self.part.clone())
    //    }
    fn compute(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = (K, C)>> {
        info!("compute inside shuffled rdd");
        //        let combiners: Arc<Mutex<HashMap<K, Arc<Mutex<C>>>>> = Arc::new(Mutex::new(HashMap::new()));
        // ArcMutex is being used in combiner because it needs to sent to closure.
        // It has significant cost associated with it since it is called a lot of times.
        // Since the combining operation is done only in sequential manner, I guess this can be eliminated.
        let mut combiners: HashMap<K, Option<C>> = HashMap::new();
        //        let mp_combiners = combiners.clone();
        //        let mut combiners_lock = mp_combiners.lock();
        let merge_pair = |(k, c): (K, C)| {
            //            let old_c = combiners_lock.get(&k);
            if let Some(old_c) = combiners.get_mut(&k) {
                let old = old_c.take().unwrap();
                let input = ((old, c),);
                let output = self.aggregator.merge_combiners.call(input);
                *old_c = Some(output);
            } else {
                //                combiners_lock.insert(k, Arc::new(Mutex::new(c)));
                combiners.insert(k, Some(c));
            }
        };

        //TODO call fetch function after ShuffleFetcher is implemented
        let time = SystemTime::now();
        let fetcher = ShuffleFetcher;
        fetcher.fetch(
            self.vals.context.clone(),
            self.shuffle_id,
            split.get_index(),
            merge_pair,
        );
        let dur = time.elapsed().unwrap().as_millis();
        info!("time taken for fetching {}", dur);
        let dur = time.elapsed().unwrap().as_millis();
        info!("time taken for converting to hashset {}", dur);
        Box::new(combiners.into_iter().map(|(k, v)| (k, v.unwrap())))

        //        let res = res.collect::<Vec<_>>();
        //        let log_output = format!("inside iterator shufflerdd {:?}", res.get(0));
        //        env::log_file.lock().write(&log_output.as_bytes());
        //        Box::new(res.into_iter()) as Box<dyn Iterator<Item = (K, C)>>
    }
}
