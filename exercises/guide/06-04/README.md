
# 06-04 Inspecting the Kubernetes Dashboard

The purpose of this exercise is to tour the Kubernetes web UI and to learn the functions for reviewing logs in pods.

### 1. Open Kubernetes Dashboard

#### 1.1. Open the Kubernetes Dashboard
The minikube dashboard command will automatically open a web page to the Kubernetes Dashboard. You must leave the terminal open.

```
minikube dashboard

```

![Open the Kubernetes Dashboard](images/step-1.png)


#### 1.2. Review the Kubernetes Dashboard
The dashboard opens to the default namespace. It shows an overview of the current workloads. Here we observe two Pods are running. You may have different Pods open.

![Review the Kubernetes Dashboard](images/step-2.png)


#### 1.3. Review the Menu
The left hand column is a list of filters for the Kubernetes resources. Scroll update down this list. Many were discussed in the class.

![Review the Menu](images/step-3.png)


### 2. Open a Namespace

#### 2.1. Open a Namespace
Click on **default**. 

![Open a Namespace](images/step-5.png)


#### 2.2. Search for a Namespace
This lists all of the current running namespaces. 

Select the namespace for **app**.

![Search for a Namespace](images/step-6.png)


### 3. Inspect the Pod

#### 3.1. Select the Pod
Click on **web-pod.**

![Select the Pod](images/step-8.png)


#### 3.2. Inspect the Pod Details
Scroll up and down the page to inspect the Pod details.  At the bottom of the page you will find the information about the container.

![Inspect the Pod Details](images/step-9.png)


#### 3.3. Open the Logs
Click on the icon for logs. 

![Open the Logs](images/step-10.png)


#### 3.4. Review the Logs
Review the logs. When done click on the breadcrum trail to return to **flask-pod**.

![Review the Logs](images/step-11.png)


#### 3.5. Open the Shell
Click on the icon for shell. If a terminal is available this will execute into the pod.

![Open the Shell](images/step-12.png)


#### 3.6. Inspect the Container
```
ls

```

```
cat Dockerfile

```

When done click on the breadcrum trail to return to **flask-pod**.

![Inspect the Container](images/step-13.png)


#### 3.7. Open the Configuration
Click on the icon for configuration. This will display the mainifest file for the Pod.

![Open the Configuration](images/step-14.png)


#### 3.8. Review the Manifest File
The mainifest file can be formatted as YAML or as JSON. Review both.

Click **Cancel** when done.

![Review the Manifest File](images/step-15.png)


### 4. Inspect the Service

#### 4.1. Open Services
Click on **Services** in the left hand table of contents. Click on the** flask-svc.**

![Open Services](images/step-17.png)


#### 4.2. Review the Service
Scroll up and down the page to review the service. 

![Review the Service](images/step-18.png)


#### 4.3. Review the Service Mainifest File 
Click on the edit icon to review the manifest file. Scroll to find the properties for the ports. The host port is set to 32082, the service port is set to 8040, and the Pod port is set to 5000.

Click **Cancel** when done.

![Review the Service Mainifest File ](images/step-19.png)


### 5. Clean Up

#### 5.1. Close Dashboard
Close the tab for the Kubernetes Dashboard. Use **Ctrl-C** to exit the minikube service tunnel.

![Close Dashboard](images/step-21.png)


#### 5.2. Delete Flask Pods and Services

---

**⚠️ Do Not Delete service/kubernetes**

Do not delete the service kubernetes in the default namespace.

---

```
kubectl -n app delete pod flask-pod

```

```
kubectl -n app delete service flask-svc

```

![Delete Flask Pods and Services](images/step-22.png)


#### 5.3. Delete Web Pods and Services
```
kubectl delete pod web-pod

```

```
kubectl delete service web-svc

```

![Delete Web Pods and Services](images/step-23.png)


### 6. End of Exercise


