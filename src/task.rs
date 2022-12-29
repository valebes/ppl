pub enum Task<T: Send> {
    NewTask(T),
    Dropped,
    Terminate,
}
