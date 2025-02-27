use native_spark::*;
#[macro_use]
extern crate serde_closure;

#[test]
fn test_make_rdd() {
    // for distributed mode, use Context::new("distributed")
    let sc = Context::new("local");
    let col = sc.make_rdd((0..10).collect::<Vec<_>>(), 32);
    //Fn! will make the closures serializable. It is necessary. use serde_closure version 0.1.3.
    let vec_iter = col.map(Fn!(|i| (0..i).collect::<Vec<_>>()));
    let res = vec_iter.collect();
    sc.drop_executors();

    let expected = (0..10)
        .map(|i| (0..i).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    println!("{:?}", res);
    assert_eq!(expected, res);
}

#[test]
fn test_take() {
    let sc = Context::new("local");
    let col1 = vec![1, 2, 3, 4, 5, 6];
    let col1_rdd = sc.parallelize(col1, 4);

    let taken_1 = col1_rdd.take(1);
    assert_eq!(taken_1.len(), 1);

    let taken_3 = col1_rdd.take(3);
    assert_eq!(taken_3.len(), 3);

    let taken_5 = col1_rdd.take(7);
    assert_eq!(taken_5.len(), 6);

    let col2: Vec<i32> = vec![];
    let col2_rdd = sc.parallelize(col2, 4);
    let taken_0 = col2_rdd.take(1);
    assert!(taken_0.is_empty());
}

#[test]
fn test_first() {
    let sc = Context::new("local");
    let col1 = vec![1, 2, 3, 4];
    let col1_rdd = sc.parallelize(col1, 4);

    let taken_1 = col1_rdd.first();
    assert!(taken_1.is_ok());

    // TODO: uncomment when it returns a proper error instead of panicking
    // let col2: Vec<i32> = vec![];
    // let col2_rdd = sc.parallelize(col2, 4);
    // let taken_0 = col2_rdd.first();
    // assert!(taken_0.is_err());
}
