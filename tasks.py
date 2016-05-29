from invoke import run, task


@task
def protoc():
    run("protoc --proto_path=proto --python_out=. `find proto -name '*.proto'`")
