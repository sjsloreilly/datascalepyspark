## Welcome to Data at Scale with Pyspark!  
We're looking forward to the course. If you have any questions please contact us:  
Sahil Jhangiani: s.s.jhangiani@gmail.com  
Sev Leonard: sev@thedatascout.com  


## Setup
To make the best use of class time, complete the following instructions ahead of class. 

It can take 24 hours for your account to be active. For best results sign up a few days before class.

### IMPORTANT NOTE: There will be some AWS fees incurred if you choose to go through the course exercises. Please read the instructions very carefully. 

#### Clone the course GitHub repo locally
1. Clone https://github.com/sjsloreilly/datascalepyspark to your local machine.

#### Create an AWS account 
1. Go to https://portal.aws.amazon.com/billing/signup#/start.
1. Select “personal” account (rather than professional).
1. You’ll need to provide a credit card to sign up.
1. Select the free plan.
1. Click Sign in to the Console and use the credentials you just created to sign in as a Root user

#### Create an S3 bucket
We’ll use this for storing files during the class.
1. In the Find Services search box, type “S3” and select “S3 scalable storage in the cloud.”
1. Click Create Bucket. 
    1. Name it “data-scale-oreilly-{your name}”  We will refer to this as your bucket name for the rest of the setup instructions.
    1. Region: US East (N Virginia)
    1. Click the "Create" button in the lower right corner
1. Click on the bucket name and click “Create folder” to create the following folders. Leave Server Side Encryption disabled
    - emr-logs
    - notebooks
    - data 
1. Under the data folder create another folder borough-zip-mapping
    1. Click the borough-zip-mapping folder to open it
    1. Click Upload and upload the ny-zip-codes.csv file from the github repo you cloned earlier.
1. Under the notebooks folder upload the bootstrap.sh file from the github repo you cloned earlier.

### Create a notebook cluster
1. In the top left corner click "Services" 
1. In the search box under "All Services" type "EMR" and select "EMR Managed Hadoop Framework" to navigate to the EMR console
1. In the upper right hand corner, ensure the N. Virginia region us-east-1 is selected.
1. On the left sidebar click Notebooks and Create Notebook.
1. Notebook name: data-scale-oreilly-notebook
    1. Cluster: select Create a cluster
        - Cluster name: data-scale-oreilly-notebook-cluster
    1. Notebook location: select Choose an existing S3 location in us-east-1 and enter s3://{your bucket name}/notebooks
1. Click Create Notebook.
This will create a new cluster which we can terminate and customize for class.
1. In the sidebar, select Clusters. You should see “data-scale-oreilly-notebook-cluster” in the cluster list. You may have to reload the page. Click on the cluster name to open the cluster detail view.
1. Click Terminate and then Clone. It is OK to terminate the cluster while it is starting.
1. For the dialog asking if you want to clone steps, select No.
1. Click the Previous button on the lower right side of the cluster setup screen until you get back to Step 1: Software and Steps in the sidebar on the left side.
    1. Under Software Configuration change the Release to "emr-5.29.0"
    1. Under Edit Software settings click the radio button for Enter configuration. Paste the following into the text area:
`[{"classification":"spark-defaults","properties":{"spark.driver.memory":"10G","spark.driver.maxResultSize":"5G"}}]`
1. Click the Next button to Step 2: Hardware
    1. Under Cluster Nodes and Instances, for Core node type: 
        1. Change the Instance type to m5.2xlarge 
        1. Set the Instance count to 2
        1. Set the EBS Storage to 128 GiB
1. Click Next.
1. Under General Options:
    1. Change the logging S3 folder to “s3://{your bucket name}/emr-logs.”
    1. Check the box by Debugging to enable.
    1. Further down the page, under Additional Options, expand the Bootstrap Actions section.  
        a. Next to Add bootstrap action select Custom action from the drop down  
        b. Click Configure and add  
        c. Name the action "Install dependencies"  
        d. For the Script location, enter "s3://{your bucket name}/notebooks/bootstrap.sh"  
        e. Click Add
1. Click Next and then Create Cluster.
The notebook will need to be associated with this new version of the cluster, so select Notebooks from the sidebar. The notebook you created should be stopped.
1. Click on the name of the notebook to open the detail view. 
    1. Click Change cluster and select the cluster created by the cloning process. 
    1. Click Change cluster and Start Notebook.
Once the notebook starts, the Open in JupyterLab button will be enabled. Click it to launch JupyterHub in a new tab.

1. Upload course notebooks  
In the JupyterLab UI launched in the previous step, you can upload the course notebooks from your local machine by clicking the Upload Files icon.  

You cannot select a directory, but you can select all notebooks in the a directory for upload by holding down shift while clicking the files.
That’s it! If you’ve gotten this far, you’ve successfully set up for class. If you’d like to try running a notebook from the course, be sure to set the kernel to PySpark.

**IMPORTANT**—stop the notebook and terminate the cluster. If you don’t, you’ll pay for the cluster uptime.

To stop the notebook:  
1. Close the JupyterLab tab
1. In the Notebooks console where you clicked Open in JupyterLab click the Stop button.

To stop the cluster:
1. Click Clusters in the sidebar
1. Click the checkbox to the left of the running data-scale-oreilly-notebook-cluster and click Terminate. Confirm by clicking Terminate in the popup window.

To track your AWS spending:
1. Click Services in the top left corner of the EMR console
1. Enter “Billing” into the text box and click Billing
This will update in about 24hours with the accumulated costs.

Estimated AWS resource cost per attendee: 5 hours * (3 nodes * ($.448 EC2 + $.096 EMR)) + $.12 EBS = ~$8.28 for the class and preclass setup
