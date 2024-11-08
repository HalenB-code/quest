struct Resource {
    variant: ResourceType,
  }
  
  #[derive(Debug)]
  enum ResourceType {
    Coordinator,
    Agent
  }
  
  impl Resource {
    fn initiate_response(self) {
      println!("I am a {:?}", self.variant);
    }
  }