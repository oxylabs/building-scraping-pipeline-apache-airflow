from bootstrap import queue, client

jobs = client.create_jobs(
    [
        "https://sandbox.oxylabs.io/products/1",
        "https://sandbox.oxylabs.io/products/2",
        "https://sandbox.oxylabs.io/products/3",
        "https://sandbox.oxylabs.io/products/4",
        "https://sandbox.oxylabs.io/products/5",
    ]
)


for job in jobs["queries"]:
    queue.push(job["id"])
    print("job id: %s" % job["id"])
