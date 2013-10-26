# denim

denim is a platform for distributed computing in Python. It is designed as a
robust, efficient alternative to solutions such as RabbitMQ and celery.

## Design philosophy

    * Ease of implementation of distributed systems
    * Robust services and bounded queues
    * Predictable scalability and degradation
    * Ease of integration with existing systems

## Ease of implementation

denim requires a minimum of configuration to set up. A single manager can
manage any number of worker nodes. Workers automatically notify their manager
of their availability and begin servicing tasks immediately. The manager
ensures that work is distributed in a sensible fashion across workers in
order to maximize throughput.

## Robust services and bounded queues

denim is designed to ensure that services can function independently of each
other. A service crashing or network lag will never cause other hosts to crash.

Workers that are restarted are gracefully handled by the manager, picking up
slack as soon as they come back on line. Likewise, a restarted manager will
pick back up where it left off, with its workers immediately reestablishing
their connections as soon as it becomes available again.

Additionally, task queues in a denim system are bounded to protect the system
from DOS-style attacks or from simply being overwhelmed by legitimate work.
This prevents queues from growing overlarge due to spikes in traffic, resulting
in a secondary delay in restoring responsiveness while the backlog of queued
tasks is processed. This ensures that the server and workers remain at peak
responsiveness and never thrash their host systems, which can result in
slowness due to paging and backlogged disk activity.

Tasks that cannot be queued due to a lack of resources will be rejected and
automatically retried by the client.

## Predictable scalability and degradation

Because queues are strictly controlled, an administrator can easily estimate
performance requirements based on the current performance of a system. Adding
another worker node to the system will provide a linear increase in performance.

For example, in a system with a single manager and two worker nodes, the
addition of another worker node will result in a 50% increase in performance.

Likewise, as a system begins to reach maximum processing capacity, the
performance of workers and the time taken to process individual tasks will
not change.

Instead, the client producing the tasks for the system will experience delays
as tasks are rejected. The client automatically retries the tasks until they
are accepted. This process is seamless and transparent to code using the client
class. Integrating code never needs to worry about handling errors caused by
busy systems.

## Ease of integration

denim is written in Python with a single point of integration - the client
class. Integrating code connects to the service and begins sending tasks. No
extra work is required outside of ensuring that the necessary application
libraries are available on worker systems.
