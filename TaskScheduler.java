package com.java.samples;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskScheduler {
	
	// default values
	private int mConcurrentExecutingTasksCount = 10;
	private int mReadyTasksCount = 10;
	
	public static enum TaskStatus{
		SCHEDULED, RUNNING, UNKNOWN;
	}
	
	// Using thread safe data structures
	private ConcurrentHashMap<Integer, Runnable> mTaskIdMap;
	private LinkedBlockingQueue<Runnable> mTaskScheduledQueue;
	private List<Runnable> mRunningTasks;
	
	private Executor mExecutor = null;
	
	private int taskIdIncrementor = 1;
	
	// Default constructor
	public TaskScheduler() {
		mTaskIdMap = new ConcurrentHashMap<Integer, Runnable>();
		mTaskScheduledQueue = new LinkedBlockingQueue<Runnable>();
		mRunningTasks = Collections.synchronizedList(new ArrayList<Runnable>());
		initializeExecutor();
	}
	
	// Constructor with given task count 
	public TaskScheduler(int concurrentExecutingTasksCount, int readyTasksCount) {
		mTaskIdMap = new ConcurrentHashMap<Integer, Runnable>();
		mConcurrentExecutingTasksCount = concurrentExecutingTasksCount;
		mReadyTasksCount = readyTasksCount;
		mTaskScheduledQueue = new LinkedBlockingQueue<Runnable>();
		mRunningTasks = Collections.synchronizedList(new ArrayList<Runnable>());
		initializeExecutor();
	}
	
	private void initializeExecutor() {
		mExecutor = new ThreadPoolExecutor(mConcurrentExecutingTasksCount,
				mReadyTasksCount, 0, TimeUnit.MILLISECONDS, mTaskScheduledQueue,
				Executors.defaultThreadFactory()) {
			
			// callback invoked before task is about to be executed
			@Override
			protected void beforeExecute(Thread t, Runnable r) {
				super.beforeExecute(t, r);
				mRunningTasks.add(r);
			}

			// callback invoked after task is successfully executed
			@Override
			protected void afterExecute(Runnable r, Throwable t) {
				super.afterExecute(r, t);
				mRunningTasks.remove(r);
				for(Entry<Integer, Runnable> entry: mTaskIdMap.entrySet()) {
					if(entry.getValue() == r) {
						mTaskIdMap.remove(entry.getKey());
						break;
					}
				}
			}
		};
	}
	
	protected int StartTask(Runnable task) throws Exception {
		if(null == task)
			throw new Exception("task should not be null");
		// taskId is -1 if task can't be added to any queue - reject the task
		if(mRunningTasks.size() == mConcurrentExecutingTasksCount && mTaskScheduledQueue.size() == mReadyTasksCount) {
			return -1;
		}
		int taskId = taskIdIncrementor++;
		mTaskIdMap.put(taskId, task);
		// task will be added to queue, if max number of tasks are being executed in parallel
		mExecutor.execute(task);
		return taskId;
	}
	
	protected TaskStatus GetTaskStatus(int taskId) {
		Runnable runnable = mTaskIdMap.get(taskId);
		if(null == runnable)
			return TaskStatus.UNKNOWN;
		for(Runnable r: mTaskScheduledQueue) {
			if(runnable == r)
				return TaskStatus.SCHEDULED;
		}
		for(Runnable r: mRunningTasks) {
			if(runnable == r)
				return TaskStatus.RUNNING;
		}
		return TaskStatus.UNKNOWN;
	}
	
	protected boolean KillTask(int taskId) {
		Runnable runnable = mTaskIdMap.get(taskId);
		if(null == runnable)
			return false;
		// remove the task from queue if it is scheduled else false
		// considering the task that is being executed can't be killed
		for(Runnable r: mTaskScheduledQueue) {
			if(runnable == r) {
				mTaskScheduledQueue.remove(r);
				mTaskIdMap.remove(taskId);
				return true;
			}
		}
		return false;
	}
	
	protected void PrintAllTaskStatus() {
		for(Entry<Integer, Runnable> entry: mTaskIdMap.entrySet()) {
			// if task is not in the running list, it should be in the schedule task queue;
			if(mRunningTasks.contains(entry.getValue())) {
				System.out.println("Task Id: " + entry.getKey() + ", Status: Running");
			} else{
				System.out.println("Task Id: " + entry.getKey() + ", Status: Scheduled");
			}
		}
	}
}
