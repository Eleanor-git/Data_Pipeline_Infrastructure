from prefect import flow, task


@task(name="greeting")
def greeting(name: str = "manager"):
    msg = f"Hello, {name}!"
    print(msg)
    return msg


@flow(name="simple_workflow_demo", log_prints=True)
def simple_workflow_demo(name: str = "manager"):
    print("Flow started - about to run the first task.")
    result = greeting(name)

    print("Flow completed")
    return result


if __name__ == '__main__':
    result = simple_workflow_demo("world")
    print(f"Result: {result}")
