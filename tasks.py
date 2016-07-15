from invoke import run, task


@task
def protoc(context):
    run("protoc --proto_path=proto --python_out=. `find proto -name '*.proto'`")
