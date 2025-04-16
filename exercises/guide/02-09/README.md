
# 02-09 Managing KeyTabs

To run queries on the `SQL Stream Builder` you need to have your KeyTab `unlocked`. This is mainly for `authentication` purposes. As the credential you are using is sometimes reused as part of other people doing the same lab it is possible that your KeyTab is `already unlocked`. We have shared the steps for both the scenarios.

### 1. Unlock your KeyTab  <a name="nU7cVs99fz0GYe_CULAmo"></a>

#### 1.1. Click on Menu   <a name="W6QtdyDubOAlQIT8yEY-E"></a>
Select the Menu option by clicking on the 9 dots. 

![Click on Menu ](images/step-1.png)


#### 1.2. Select Management Console   <a name="mdDUNakQNFHckieao5sM_"></a>
Navigate to the Management Console page by clicking the Management Console tile.

![Select Management Console ](images/step-2.png)


#### 1.3. Select Environment   <a name="FncoZXw6UtNOJyw60ffcf"></a>
Click on the environment. 

![Select Environment ](images/step-3.png)


#### 1.4. Select Analytics Data Hub   <a name="Rru7vfF6duWxUG_TpBnjc"></a>
Click on the Data Hub cluster for stream analytics. (Ex: edu-ds-analytics-250204) 

![Select Analytics Data Hub ](images/step-4.png)


#### 1.5. Click Streaming SQL Console.  <a name="KUyoB1GkFFv-JloDgmUI0"></a>
Open the SSB UI by clicking on `Streaming SQL Console`.

![Click Streaming SQL Console.](images/step-5.png)


#### 1.6. Click on the User name  <a name="2yzDVDW49E7VoL8mIsXI0"></a>
Click on the User name (Ex: `dse_2_250204`) at the bottom left of the screen and select `Manage Keytab`. Make sure you are logged in as the username that was assigned to you.

![Click on the User name](images/step-6.png)


#### 1.7. Enter Credentials  <a name="gvqtYZ6SLyW3cht2QgQXb"></a>
Enter your Workload Username under `Principal Name *` and workload password that you had set earlier in the `Password *` field.

Click on **Unlock Keytab.**

![Enter Credentials](images/step-7.png)


#### 1.8. Close  <a name="9ee3n-ozn2t7z8nJc3dO8"></a>
A message appears confirming 'Success KeyTab has been unclocked'. 

Click on **X **to close the window. 

![Close](images/step-8.png)


### 2. Reset your KeyTab  <a name="X2Ac2kG5aw3hmJMj6kLJl"></a>

#### 2.1. Click on the User name  <a name="b2_fK1UDzslmLoybD30Bi"></a>
Click on the User name (Ex: `dse_2_250204`) at the bottom left of the screen and select `Manage Keytab`. Make sure you are logged in as the username that was assigned to you.

![Click on the User name](images/step-10.png)


#### 2.2. Verify Keyab  <a name="GXjaiQDUE_4c8M2X_q-jx"></a>
If you get the following dialog box it means that your Keytab is already `UNLOCKED`. 

Hence, it would be necessary to reset here by locking it and unlocking it again using your newly set workload password.

Click on **Lock Keytab.**

![Verify Keyab](images/step-11.png)


#### 2.3. Review message  <a name="0ioG3KcGr9L3XU2VtmNZ6"></a>
A success message appears confirming Keytab has been locked.

Click on **X **to close the window. 

![Review message](images/step-12.png)


#### 2.4. Click on the User name  <a name="haQJpfH6zNv319VlOiEpN"></a>
Click on the User name (Ex: `dse_2_250204`) at the bottom left of the screen and select `Manage Keytab`.

![Click on the User name](images/step-13.png)


#### 2.5. Enter Credentials   <a name="wnHqp51DRvuwN1kMZ90DL"></a>
Enter your Workload Username under `Principal Name *` and workload password that you had set earlier in the `Password *` field.

Click on **Unlock Keytab.**

![Enter Credentials ](images/step-14.png)


#### 2.6. Close   <a name="tpZ9fDOwkVhfc1QbgipQr"></a>
A message appears confirming 'Success KeyTab has been unclocked'.

Click on **X **to close the window. 

![Close ](images/step-15.png)


### 3. End of the Exercise   <a name="8R3ZvQ8FIoO7xwLNSbg90"></a>


