struct Session {
    unique_id: String,
    duration: usize,
    execution_statistics: HashMap<String, usize>,
    coordinator_id: String,
    agent_pool: HashMap<usize, String>,
    session_query_plan: HashMap<usize, String>
}

trait SessionActions {
    pub fn start_session();
    pub fn end_session();
    pub fn execute_query_plan();
    pub fn instantiate_coordinator();
    pub fn instantiate_execution_agents();
}