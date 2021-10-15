pub enum Dependency {
    Narrow(NarrowDependency),
    Shuffle(ShuffleDependency),
}

pub enum NarrowDependency {
    OneToOne(OneToOneDependency),
}

pub struct OneToOneDependency {}

pub enum ShuffleDependency {}
