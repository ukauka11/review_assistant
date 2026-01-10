task_name = input("Task name: ")
priority = input("Priority (high/medium/low): ")

task = {
    "name": task_name,
    "priority": priority
}

if task["priority"] == "high":
    print(" Do this task FIRST:", task["name"])
elif task["priority"] == "medium":
    print("Do this tas soon:", task["name"])
else:
    print("This can wait:", task["name"])