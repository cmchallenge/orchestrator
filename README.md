# Orchestrator

ClearMetal runs on a distributed services architecture.  This architecture has various components that need to be synchronized to complete our daily pipeline.  

In order to run this pipeline, we need an orchestrator that can schedule and synchronize tasks to be run.  

Your job is to create this orchestrator.  The guidelines for the orchestrator are:
- A task can be either recurring or one-time
- A task can have dependencies.  For example, TaskABC may need to be executed before TaskXYZ can start
- The result of each task run should be recorded (i.e. did the task succeed or fail)
- You can design the tasks however you want (i.e. they can be a bash script, a python script, etc…).   
- You should have at least one test that runs an example set of tasks

We ask that you use Python for this challenge, as that is the primary language used for ClearMetal's backend.   

This is designed to be a fairly open-ended challenge, which means there are a lot of directions you can take it (distributed task processing, complex dependency checking, graceful failures, retries, etc.).  We ask that you not spend too much time on it as we are mostly evaluating your thought process and your code design rather than a fully working framework.
