use super::*;
//use std::hash::Hash;
use std::hash::Hash;
use std::sync::Arc;
//use std::any::Any;

// This module implements parallel collection RDD for dividing the input collection for parallel processing

#[derive(Serialize, Deserialize, Clone)]
pub struct ParallelCollectionSplit<T> {
    rdd_id: i64,
    index: usize,
    values: Arc<Vec<T>>,
}

impl<T: Data> Split for ParallelCollectionSplit<T> {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl<T: Data> ParallelCollectionSplit<T> {
    fn new(rdd_id: i64, index: usize, values: Arc<Vec<T>>) -> Self {
        ParallelCollectionSplit {
            rdd_id,
            index,
            values,
        }
    }
    // Lot of unnecessary cloning is there. Have to refactor for better performance
    fn iterator(&self) -> Box<dyn Iterator<Item = T>> {
        let data = self.values.clone();
        let len = data.len();
        Box::new((0..len).map(move |i| data[i].clone()))
        //        let res = res.collect::<Vec<_>>();
        //        let log_output = format!("inside iterator maprdd {:?}", res.get(0));
        //        env::log_file.lock().write(&log_output.as_bytes());
        //        Box::new(res.into_iter()) as Box<Iterator<Item = T>>
        //        Box::new(self.values.clone().into_iter())
    }
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T> {
    vals: Arc<RddVals>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Context,
    //    data: Option<Vec<T>>,
    splits_: Vec<Arc<Vec<T>>>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T> {
    rdd_vals: Arc<ParallelCollectionVals<T>>,
}

impl<T: Data> Clone for ParallelCollection<T> {
    fn clone(&self) -> Self {
        ParallelCollection {
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

impl<T: Data> ParallelCollection<T> {
    pub fn new(context: Context, data: Vec<T>, num_slices: usize) -> Self {
        ParallelCollection {
            rdd_vals: Arc::new(ParallelCollectionVals {
                vals: Arc::new(RddVals::new(context.clone())),
                context,
                splits_: ParallelCollection::slice(data, num_slices),
                num_slices,
            }),
        }
    }
    fn slice(data: Vec<T>, num_slices: usize) -> Vec<Arc<Vec<T>>> {
        if num_slices < 1 {
            panic!("Number of slices should be greater than or equal to 1");
        } else {
            let mut slice_count = 0;
            let data_len = data.len();
            //let mut start = (count * data.len()) / num_slices;
            let mut end = ((slice_count + 1) * data_len) / num_slices;
            let mut output = Vec::new();
            let mut tmp = Vec::new();
            let mut iter_count = 0;
            for i in data {
                if iter_count < end {
                    tmp.push(i);
                    iter_count += 1;
                } else {
                    slice_count += 1;
                    end = ((slice_count + 1) * data_len) / num_slices;
                    output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
                    tmp.push(i);
                    iter_count += 1;
                }
            }
            output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
            output
            //            data.chunks(num_slices)
            //                .map(|x| Arc::new(x.to_vec()))
            //                .collect()
        }
    }
}
impl<K: Data, V: Data> RddBase for ParallelCollection<(K, V)> {
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any parallel collection",);
        Box::new(
            self.iterator(split)
                .map(|(k, v)| Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>),
        )
    }
}

impl<T: Data> RddBase for ParallelCollection<T> {
    fn get_rdd_id(&self) -> usize {
        self.rdd_vals.vals.id
    }
    fn get_context(&self) -> Context {
        self.rdd_vals.vals.context.clone()
    }
    fn get_dependencies(&self) -> &[Dependency] {
        &self.rdd_vals.vals.dependencies
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        //        let slices = self.slice();
        (0..self.rdd_vals.splits_.len())
            .map(|i| {
                Box::new(ParallelCollectionSplit::new(
                    self.rdd_vals.vals.id as i64,
                    i,
                    self.rdd_vals.splits_[i as usize].clone(),
                )) as Box<dyn Split>
            })
            .collect::<Vec<Box<dyn Split>>>()
    }
    fn number_of_splits(&self) -> usize {
        self.rdd_vals.splits_.len()
    }

    default fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        self.iterator_any(split)
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = Box<dyn AnyData>>> {
        info!("inside iterator_any parallel collection",);
        Box::new(
            self.iterator(split)
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        )
    }
}

impl<T: Data> Rdd<T> for ParallelCollection<T> {
    fn get_rdd(&self) -> Arc<Self> {
        Arc::new(ParallelCollection {
            rdd_vals: self.rdd_vals.clone(),
        })
    }
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn compute(&self, split: Box<dyn Split>) -> Box<dyn Iterator<Item = T>> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<T>>() {
            s.iterator()
        } else {
            panic!(
                "Got split object from different concrete type other than ParallelCollectionSplit"
            )
        }
    }
}

//impl<K: Data + Eq + Hash, V: Data + Eq + Hash> PairRdd<K, V> for ParallelCollection<(K, V)> {}
