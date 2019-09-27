# ccbcutrackingest
This is POC code for CCBCU to implement Azure kubernetes / Azure Container using Flask web application, SQLAlchemy and PYODBC in ubuntu 16.4 environment


####Example for writing log to application insight

#app.logger.debug('This is a debug log message')

#app.logger.info('This is an information log message')

#app.logger.warn('This is a warning log message')

#app.logger.error('This is an error message')

#app.logger.critical('This is a critical message')


Reference Link: https://docs.microsoft.com/en-us/azure/aks/tutorial-kubernetes-deploy-cluster

###################################################################################################

ENVIRONMENT: Visual Studio Code

Below are the steps to deploying CCBCU Track Ingest service to Kubernetes 

0. Logon to Azure and set your subscription
  
  i.  az login
  
  ii. az account set --subscription <Azure subscription ID>   

1. Test python code locally

  i. Run CMD in terminal: python runserver.py
  
  ii. go to browser and type http://127.0.0.1:5000. You should see "CCBCU Track Ingest Service"
  
  iii. CTL + C to terminate application
  

2. build docker image and test locally: 

  i. run CMD: docker-compose build
  
  ii.run CMD: docker-compose up
  
  iii. go to browser and type http://127.0.0.1:5000. You should see "CCBCU Track Ingest Service"
  
  iv. CTL + C to terminate application
  
  v. run CMD: docker-compose down (this will clean up resource from your local machine)


4. Deploy to Azure Container Registry

   run the following commands in order below: 

   i.  az login 
   
   ii. az account set --subscription <Azure subscription ID> 
  
   iii.az group create --name ccbcutrackingest-eastus-rg --location eastus
   
   vi. az acr create --resource-group ccbcutrackingest-eastus-rg --name ccbcutrackingesteastusacr --sku Basic
   
   vii.az acr login --name ccbcutrackingesteastusacr
   
   viii. az acr list --resource-group ccbcutrackingest-eastus-rg --query "[].{acrLoginServer:loginServer}" --output table
   
   ix. docker tag ccbcutrackingest ccbcutrackingesteastusacr.azurecr.io/ccbcutrackingesteastus:v1
   
   x. docker images
   
   xi.docker push ccbcutrackingesteastusacr.azurecr.io/ccbcutrackingesteastus:v1
   
   xii. az acr repository list --name ccbcutrackingesteastusacr --output table
   
   xiii. az acr repository show-tags --name ccbcutrackingesteastusacr --repository ccbcutrackingesteastus --output table


5. Create Azure Kubernetes Environment

    i. az ad sp create-for-rbac --skip-assignment
    
    ii. copy output from step i in a notepad
    
          Example:
          
            "appId": "<app ID>",
            
            "displayName": "azure-cli-2019-05-16-21-29-30",
            
            "name": "http://azure-cli-2019-05-16-21-29-30",
            
            "password": "<Secret ID>",
            
            "tenant": "<Tenant ID>"
            
            
    iii. az acr show --resource-group ccbcutrackingest-eastus-rg --name ccbcutrackingesteastusacr --query "id" --output tsv
    
          copy output to notepad: 
          
          /subscriptions/<Azure subscription ID>/resourceGroups/ccbcutrackingest-eastus-rg/providers/Microsoft.ContainerRegistry/registries/ccbcutrackingesteastusacr
          
    iv. az role assignment create --assignee <app ID> --scope /subscriptions/<Azure subscription ID>/resourceGroups/ccbcutrackingest-eastus-rg/providers/Microsoft.ContainerRegistry/registries/ccbcutrackingesteastusacr --role acrpull
  
    v. az aks create --resource-group ccbcutrackingest-eastus-rg --name ccbcutrackingesteastuscluster --node-count 1 --service-principal <app ID> --client-secret <Secret ID> --generate-ssh-keys
  
    vi. az aks install-cli
    
    vii. az aks get-credentials --resource-group ccbcutrackingest-eastus-rg --name ccbcutrackingesteastuscluster
    
    viii. kubectl get nodes


6. Deploy Application to Kubernetes and test

    i. kubectl apply -f ../yaml/serviceeastus.yaml
    
    ii. kubectl get service ccbcutrackingesteastus --watch
    
    iii. copy Extermal-IP address of ccbcutrackingesteastus and paste in browser to test


7. Enable Client IP for SQL Server

    i. run kubectl get services
    
    ii. take the external IP address of ccbcutrackingesteastus and add that to firewall of Azure DB on portal.azure.com

###################################################################################################

Commands / Steps that might come handy

1. Steps to run kubectl proxy from visual studio code terminal
  
  i. kubectl proxy
  
  ii. go to web browser and run http://localhost:8001/api/v1/namespaces/kube-system/services/kubernetes-dashboard/proxy/#!/overview?namespace=default
  

Note: If web browser fails to load proxy site run the command below in visual studio terminal and run step ii above

CMD: kubectl create clusterrolebinding kubernetes-dashboard --clusterrole=cluster-admin --serviceaccount=kube-system:kubernetes-dashboard


2. how to see the details of a pod?

   kubectl describe pod ccbcutrackingest-service-5456b8876f-7s622

3. how to delete pod forcefully?

   kubectl delete pods ccbcutrackingest-service-5456b8876f-zjv8r --grace-period=0 --force

4. how to delete a service?

   kubectl delete service ccbcutrackingest-service
