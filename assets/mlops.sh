#!/bin/bash
# Remove older versions of Docker if present
echo "Removing older versions of Docker..."
sudo yum remove -y docker \
                  docker-client \
                  docker-client-latest \
                  docker-common \
                  docker-latest \
                  docker-latest-logrotate \
                  docker-logrotate \
                  docker-engine
# Install Git
echo "Installing Git 2.4.3 version..."
sudo yum install git-2.43.5 -y
# Verify Git installation
git --version && echo "Git has been installed successfully!"
# Add Docker repository
echo "Adding Docker repository..."
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
# Disable updates for Docker in yum to prevent automatic updates to unsupported versions
sudo yum-config-manager --save --setopt=docker-ce.skip_if_unavailable=true
# Manually download Docker 26.1.3 RPM packages and its dependencies
mkdir ~/installers
cd ~/installers/
echo "Downloading Docker 26.1.3..."
curl -LO https://admin-public.s3.amazonaws.com/rhel8-packages/docker-rpms-rhel8.tar.gz
tar -zxvf docker-rpms-rhel8.tar.gz
# Install Docker version 26.1.3
echo "Installing Docker 26.1.3..."
cd docker
sudo yum install *.rpm -y
# Start and enable Docker service
sudo systemctl start docker
sudo systemctl enable docker
# Add current user to the docker group
echo "Adding current user to the Docker group..."
sudo usermod -aG docker $USER
newgrp docker # Apply the new group membership immediately
# Adjust Docker socket permissions
sudo chmod 666 /var/run/docker.sock
# Verify Docker installation
docker --version && echo "Docker 26.1.3 has been installed successfully!"
# Download kubectl
cd ~/installers/
echo "Downloading kubectl v1.31.1..."
curl -LO "https://dl.k8s.io/release/v1.31.1/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/
# Verify kubectl installation
kubectl version --client && echo "kubectl has been installed successfully!"
# Download and install Minikube
cd ~/installers/
echo "Downloading and installing Minikube Dependencies..."
curl -LO https://admin-public.s3.amazonaws.com/rhel8-packages/conntrack-rpms-rhel8.tar.gz
tar -zxvf conntrack-rpms-rhel8.tar.gz
cd conntrack
sudo yum install *.rpm -y
echo "Downloading and installing Minikube..."
cd ~/installers/
curl -LO https://storage.googleapis.com/minikube/releases/v1.33.1/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
minikube start
# Verify Minikube installation
minikube version && echo "Minikube has been installed successfully!"