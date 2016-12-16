Task Scheduler supports the following tasks
1. Start a task
2. Get a task status
3. Kill a task
4. Print all task statuses

Task Scheduler is capable of executing the M tasks concurrently, and scheduling N tasks. If a task is scheduled, it will be executed as soon as a running task has been executed.
StartTask returns -1 if task can't be added (Task scheduler is executing M tasks and scheduled N tasks). A task that is being executed can't be killed. Only a scheduled task can be killed.