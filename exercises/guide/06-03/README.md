
# 06-03 Running Pods and Services

The purpose of this exercise is to run Kubernetes CLI tools, which includes kubectl and K9s.

### 1. Review kubectl Usage

#### 1.1. Review kubectl Usage
Page through the help pages to overview kubectl. Identify the basic commands, such as get, edit, explain and delete. Take your time to review the help page. 

```
kubectl --help 

```

![Review kubectl Usage](images/step-1.png)


#### 1.2. Review kubectl Usage
The kubectl command has excellent usage statements. Review this usage statement to review the various example commands. Review this page for the syntax. 

```
kubectl get pods -h

```

![Review kubectl Usage](images/step-2.png)


### 2. Kubernetes Pods

#### 2.1. View All Pods
Get a list of all Pods in the Default namespace. Currently there are no Pods running.

```
kubectl get pods

```

![View All Pods](images/step-4.png)


#### 2.2. Start a Pod
Start up a Pod for nginx. The option --image has a default to pull from Docker Hub.

```
kubectl run web-pod --image nginx

```

```
kubectl get pods

```

![Start a Pod](images/step-5.png)


#### 2.3. Inspect the Pod
Inspect the Pod. Scan through to find the image name. What Port is assigned to the Pod?

```
kubectl describe pod web-pod

```

![Inspect the Pod](images/step-6.png)


#### 2.4. Review the Logs
```
kubectl logs web-pod

```

![Review the Logs](images/step-7.png)


#### 2.5. Start another Pod
Start up a Pod for postgresql

```
kubectl run db-pod --image postgres

```

```
kubectl get pods

```

![Start another Pod](images/step-8.png)


#### 2.6. List Pods for More Details
Get a list of pods using the -o wide option.

```
kubectl get pods -o wide

```

![List Pods for More Details](images/step-9.png)


#### 2.7. View Logs
A CrashLoopBackOff is a failure to start. View the logs for the failing Pod.

```
kubectl logs pgsql-pod

```

This container needs configuration. It needs to have a ConfigMap with input variables.

![View Logs](images/step-10.png)


#### 2.8. Delete a Pod
There is an issue with one of the Pods. Delete the Pod in CrashLoopBackOff.

```
kubectl delete pod db-pod

```

```
kubectl get pods

```

![Delete a Pod](images/step-11.png)


### 3. Kubernetes Services

#### 3.1. Review the Usage for service
Review the usage statement for the service.

```
kubectl expose -h

```

![Review the Usage for service](images/step-13.png)


#### 3.2. Expose the ports for nginx
The expose command will assign ports to allow access to nginx. Port 80 is for the nginx Pod and the target port opens a listener on the host.

```
kubectl expose pod web-pod --name web-svc --type NodePort --port 8080

```

```
kubectl get svc

```

![Expose the ports for nginx](images/step-14.png)


#### 3.3. Set up service tunnel
Minikube is running as a Docker container. The service is only exposed from the Pod to the minikube container. A service tunnel must be opened to allow access from the local host to the minikube container. The terminal needs to remain open to access the url. The --url will output the correct URL to access the web page.

```
minikube service web-svc --url

```

![Set up service tunnel](images/step-15.png)


#### 3.4. Verify Access to nginx
Open a tab in your browser and verify access to nginx. Copy and paste in the URL.

```
http://localhost:8888

```

When done close the tab.

![Verify Access to nginx](images/step-16.png)


#### 3.5. Exit the URL
Exit the minikube service tunnel with **Ctrl-C**.

![Exit the URL](images/step-17.png)


### 4. Load a Microservice in minikube
These steps will build the app-frontend image and then load the image into minikube.

#### 4.1. Change Directory to Dockerfile
```
cd app-flask

```

![Change Directory to Dockerfile](images/step-19.png)


#### 4.2. Build an Image
```
docker build -t app-flask .

```

![Build an Image](images/step-20.png)


#### 4.3. List Images
```
docker images

```

![List Images](images/step-21.png)


#### 4.4. Change Directory
```
cd ..

```

![Change Directory](images/step-22.png)


#### 4.5. Verify Docker Enviroment
```
minikube docker-env

```

![Verify Docker Enviroment](images/step-23.png)


#### 4.6. Set Variable
```
eval $(minikube docker-env)

```

![Set Variable](images/step-24.png)


#### 4.7. Load Image into Minikube
```
minikube image load app-flask

```

```
minikube image list

```

![Load Image into Minikube](images/step-25.png)


### 5. Kubernetes Namespaces

#### 5.1. View Namespaces
View all of the current namespaces.

```
kubectl get namespaces

```

![View Namespaces](images/step-27.png)


#### 5.2. Create a Namespace
```
kubectl create namespace app

```

```
kubectl get namespaces

```

![Create a Namespace](images/step-28.png)


#### 5.3. List the Resources in the Namespace
```
kubectl -n app get all

```

![List the Resources in the Namespace](images/step-29.png)


### 6. Run the Microservice in the Namespace

#### 6.1. Create a Pod with the Image
```
kubectl -n app run flask-pod --image app-flask --image-pull-policy=Never --port 5000 

```

- Namespace: app
- Pod name: app-pod
- Image: app-frontend
- Image pull policy: pulls the image from the local cache and not the default Docker Hub
- Port: opens the application port

```
kubectl -n app get pods

```

![Create a Pod with the Image](images/step-31.png)


#### 6.2. Inspect the Pod
```
kubectl -n app describe pod flask-pod

```

Find the Port for the Pod. It should be set to 5000/TCP.

![Inspect the Pod](images/step-32.png)


#### 6.3. Create the Service for the Pod
```
kubectl -n app expose pod flask-pod --name flask-svc --type NodePort --port 5000 --target-port 5000

```

- Namespace: app
- Pod: app-pod
- Service Name: app-svc
- Type: NodePort, this will open a listener on the minikube host
- Port: The port for the service
- Target Port: The port for the Pod

```
kubectl -n app get all

```

![Create the Service for the Pod](images/step-33.png)


#### 6.4. Inspect the Service
```
kubectl -n app describe svc flask-svc

```

![Inspect the Service](images/step-34.png)


#### 6.5. List the Endpoint
This is the IP address and Port number to reach the app service. The app service will route forward to the Pod.

```
kubectl -n app get endpoints

```

![List the Endpoint](images/step-35.png)


#### 6.6. Open the minikube Service Tunnel
```
minikube -n app service flask-svc --url

```

Copy the http://IP_address:Port Number to a clipboard.

![Open the minikube Service Tunnel](images/step-36.png)


#### 6.7. Verify Access to the Application
> http://IP_address:Port_Number

![Verify Access to the Application](images/step-37.png)


### 7. Clean Up

#### 7.1. Clean Up
Close the tab on the browser.

Use **Ctrl-C** to exit the minikube service tunnel. Leave the application running.

---

**ℹ️ Leave the Pods Running**



---


![Clean Up](images/step-39.png)


### 8. End of Exercise


