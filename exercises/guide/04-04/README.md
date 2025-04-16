
# 04-04 Running minikube

The purpose of this exercise is to start up minicube and to learn basic commands for minikube.

### 1. Open minikube

#### 1.1. Open Docker Desktop
minikkube runs in a container. It requires a container environment. Start up your container environment. In this case the image is of Docker Desktop running on macOS.

![Open Docker Desktop](images/step-1.png)


#### 1.2. Open a Terminal or IDE
Open a terminal or an IDE. This exercise will use Visual Studio Code. Note the normal color setting for VSC is white on black. For the purposes of readability on a printed page this has been changed to black on white.

![Open a Terminal or IDE](images/step-2.png)


#### 1.3. Start minikube
Start up minikube and track progress.

```
minikube start

```

Clear the screen.

```
clear

```

![Start minikube](images/step-3.png)


#### 1.4. Allow Access
If required allow VSC to access data from other apps.

![Allow Access](images/step-4.png)


### 2. Basic minikube Commands

#### 2.1. minikube Usage
```
minikube --help

```

![minikube Usage](images/step-6.png)


#### 2.2. Pause and Unpause
Pause and unpause minikube.

```
minikube pause

```

```
minikube unpause

```

![Pause and Unpause](images/step-7.png)


#### 2.3. Start and Stop
```
minikube stop

```

```
minikube start

```

![Start and Stop](images/step-8.png)


#### 2.4. Logs for minikube
```
minikube logs

```

![Logs for minikube](images/step-9.png)


#### 2.5. List Addons
Review the addons list. Addons will be configured as required.

```
minikube addons list

```

![List Addons](images/step-10.png)


#### 2.6. Enable Dashboard
```
minikube addons enable dashboard

```

![Enable Dashboard](images/step-11.png)


#### 2.7. Open Kubernetes Dashboard
Open the Kubernetes dashboard. A later exercise will review this dashboard.

```
minikube dashboard

```

When you are done close the tab for the Kubernetes dashboard. Use **Ctrl-C** to exit the command.

![Open Kubernetes Dashboard](images/step-12.png)


### 3. Deleting minikube

#### 3.1. Delete minikube clusters

---

**❌ DO NOT DELETE**

Do not delete minikube at this time. 

---

To delete all minikube clusters use this command.

```
xx minikube delete --all

```

![Delete minikube clusters](images/step-14.png)


#### 3.2. The Docker Desktop Warning
Do not delete minikube from the Docker Desktop container view. This will result in a start up error for minikube. Always delete from the command line.

![The Docker Desktop Warning](images/step-15.png)


### 4. Verify Kubernetes

#### 4.1. Test for the kubectl Command
The kubectl command should automatically be available. If it does not appear ask your instructor for an assist.

```
kubectl get pods -A

```

![Test for the kubectl Command](images/step-17.png)


#### 4.2. Run Test Commands to Create a Deployment
Create a Deployment with a simple image.

```
kubectl create deployment hello-minikube --image=kicbase/echo-server:1.0

```

```
kubectl expose deployment hello-minikube --type=NodePort --port=8080

```

These commands are also found in the = Install minikube web page.

![Run Test Commands to Create a Deployment](images/step-18.png)


#### 4.3. List Deployment and Service
Inspect the deployment

```
kubectl get deployment hello-minikube

```

```
kubectl get service hello-minikube

```

![List Deployment and Service](images/step-19.png)


#### 4.4. Open the Application

---

**ℹ️ Terminal Status**

The terminal must be left open to run the web page.

---

Use minkube to open the browser.

```
minikube service hello-minikube

```

![Open the Application](images/step-20.png)


#### 4.5. Review the Web Page
Review the web page. When done close the tab. In the IDE use **Ctrl-C** to escape the minikube service tunnel. 

```
clear

```

![Review the Web Page](images/step-21.png)


#### 4.6. Delete Deployment and Service
```
kubectl delete deployment hello-minikube

```

```
kubectl delete service hello-minikube

```

![Delete Deployment and Service](images/step-22.png)


#### 4.7. Verification Complete
The minikube cluster is now tested and ready for further exercises.

![Verification Complete](images/step-23.png)


### 5. Review minikube Documentation

#### 5.1. Open minikube Web UI
Open the minikube handbook. Scan through it to see the full range of commands and capabilities. The handbook could be used as a tutorial.

```
https://minikube.sigs.k8s.io/docs/handbook/

```

![Open minikube Web UI](images/step-25.png)


#### 5.2. Review Documentation
Select Addons > Ingress DNS. This is a good example of the value of this documentation. It provides a clear explaination with excellent example commands. The commands are demostrated for Linux, macOS, and Windows.

![Review Documentation](images/step-26.png)


### 6. End of Exercise


