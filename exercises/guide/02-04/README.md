
# 02-04 Verify permissions in Apache Ranger

!!! NOTE 
    THESE STEPS HAVE ALREADY BEEN DONE FOR YOU. This section will walk you through how Permissions/Policies are managed in Ranger. 
    
### 

!!! CAUTION 
    PLEASE DO NOT EXECUTE THE STEPS IN THIS SECTION OR CHANGE ANYTHING.

!!! important
    Since we could not grant the required privileges to the workload users to implement the steps in the environment, this exercise has been written as a read-only guide for your future reference. Please go through all the steps, screenshots, policies, permissions in detail. You won't be able to view the policies in your environment. You may request your instructor to provide a short demo.  

### 1. Accessing Apache Ranger  <a name="_MzbRVJZCz16QuKUrkjiI"></a>

#### 1.1. Click on Environments   <a name="zsdp5zylJQ8nL8ahnXgWS"></a>
Click on the `Environments` tab on the left pane in Cloudera Management Console Main panel. 

![Click on Environments ](images/step-1.png)


#### 1.2. Navigate to Management Console  <a name="RGg8rG9JxS0EaYOZsABVP"></a>
Select the environment that is shared by the instructor (Ex: `devops-570-class-250204`).

The environment assigned to you would be named similar except the class ID. 

![Navigate to Management Console](images/step-2.png)


#### 1.3. Access Ranger UI  <a name="_IOkCoQp1RYQIhuFchjGK"></a>
Click on the Ranger quick link to access the Ranger UI.

Also, make a note of the 3 Data Hubs pre-created for the exercises. We will need them in later steps.

(1) Environment should be enabled as part of the CDF Data Service - [devops-570-class-250204]

(2) Streams Messaging Data Hub Cluster should be created and running - [edu-ds-messaging-250204]

(3) Stream analytics Data Hub cluster should be created and running - [edu-ds-analytics-250204]

(4) A gateway node to access the services - [training-gateway-250204]

In your case, the class ID would be different:

- [devops-570-class-{class-id}]   
- [edu-ds-messaging-{class-id}]
- [edu-ds-analytics-{class-id}]
- [training-gateway-{class-id}]

![Access Ranger UI](images/step-3.png)


#### 1.4. Review Ranger UI  <a name="f-DBpTIEFk8HA6lWP10Mo"></a>
Notice the two data hubs are populated under KAFKA and KAFKA-CONNECT. 

![Review Ranger UI](images/step-4.png)


### 2. Kafka Permissions  <a name="463QY0XB5rFkQxA_Al08b"></a>

#### 2.1. Select the Kafka repository  <a name="vcdywF-qHXuz0lImsXD7t"></a>
In Ranger UI, select the Kafka repository that’s associated with the stream messaging datahub.

In this case, it will be [edu-ds-messaging 250204_kafka_716a]

![Select the Kafka repository](images/step-6.png)


#### 2.2. Verify user  <a name="MMGK7BKexusNsj3e8j9Ke"></a>
Verify if the user who will be performing the exercise is present in both `all-consumergroup` and `all-topic`.

A variable {USER} with all the required permissions in Apache Ranger has been added. This variable {USER} is used to represent each student in this environment in a Ranger Policy. 

The below image reflects the variable {USER} being part of `all-consumergroup`.

![Verify user](images/step-7.png)


#### 2.3. Verify user  <a name="lLCYdUHHP4bwQta7mR5n9"></a>
The below image reflects the variable {USER} being part of `all-topic`.

![Verify user](images/step-8.png)


### 3. Schema Registry Permissions  <a name="02WQIJAjuWYvtfI43GD4P"></a>
Schema Registry Permissions

#### 3.1. Navigate to Ranger Home Page  <a name="TMl9EGrfo6q3x4KotSo2p"></a>
Click on the Ranger icon in the top left corner to navigate back to the Ranger home page. 

![Navigate to Ranger Home Page](images/step-10.png)


#### 3.2. Select SCHEMA-REGISTRY  <a name="EP2FFcvAAM6l3TGvZ2zZM"></a>
In Ranger, select the `SCHEMA-REGISTRY` repository that’s associated with the stream messaging datahub.

In this case, it will be [edu_ds_messaging_250204_schemaregistry]

![Select SCHEMA-REGISTRY](images/step-11.png)


#### 3.3. Verify user  <a name="w8sI6dzvQwNKXljez1kMk"></a>
Verify if the user who will be performing the exercise is present in the Policy: all - `schema-group, schema-metadata, schema-branch, schema-version`.

A variable {USER} with all the required permissions in Apache Ranger has been added. This variable {USER} is used to represent each student in this environment in a Ranger Policy.

![Verify user](images/step-12.png)


#### 3.4. Click on Edit  <a name="FBYrNk_3yKwmPVCGg3r2W"></a>
Click on the Edit button under the Actions column to edit the policy. 

![Click on Edit](images/step-13.png)


#### 3.5. Review Policy Details  <a name="Ogg9ahYnJF8_IP6--WhAt"></a>
Review the policy details. 

![Review Policy Details](images/step-14.png)


#### 3.6. Review Allow Conditions  <a name="wNyWO-iwDqHybHh2A-ret"></a>
Verify if the user who will be performing the exercise is present in the Selected Users column. 

Review the policy permissions. 

Exit the Ranger UI by closing the tab. 

![Review Allow Conditions](images/step-15.png)


### 4. End of the Exercise   <a name="SFOpUwSs0ByPVe9jKOiUq"></a>


