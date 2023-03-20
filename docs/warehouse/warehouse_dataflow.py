from diagrams import Diagram, Edge
from diagrams.aws.integration import SimpleQueueServiceSqsQueue 
from diagrams.aws.storage import S3
from diagrams.aws.database import RDS
from diagrams.aws.compute import LambdaFunction, Fargate
from diagrams.onprem.client import Users
from diagrams.onprem.compute import Server
from diagrams.custom import Custom

with Diagram("Warehouse Dataflow"):
    # Actors
    portal = Fargate("Portal")
    portal_db = RDS("FlowMetrics DB Model")
    bucket = S3("umccr-datalake-dev")
    dracarys_gds_jobqueue = SimpleQueueServiceSqsQueue("GDS to S3 queue")
    dracarys_portal_jobqueue = SimpleQueueServiceSqsQueue("S3 to Portal DB queue")
    dracarys_dockerlambda = LambdaFunction("Dracarys R DockerLambda")
    dracarys_to_portal_lambda = LambdaFunction("Dracarys 2 portal DB")
    sequencer = Custom("Sequencer", "img/sequencer.png")
    users = Users("Curators")
    # Edge (arrows) information
    portal_run_id = Edge(label="portal_run_id")

    # Data flow
    dracarys_dockerlambda \
        >> bucket \
        >> portal_run_id \
        >> portal << portal_db
    
    sequencer \
        >> portal_run_id \
        >> portal \
        << users

    dracarys_portal_jobqueue >> portal_db
    dracarys_to_portal_lambda >> dracarys_portal_jobqueue
    dracarys_gds_jobqueue >> dracarys_dockerlambda
