Vagrant.configure(2) do |config|
  config.vm.define "integration" do |s|
    s.ssh.forward_agent = true
    s.vm.box = "ubuntu/bionic64"
    s.vm.hostname = "integration"
    s.vm.network "private_network",
      ip: "192.168.1.20",
      netmask: "255.255.255.0",
      auto_config: true
    s.vm.provision :shell,
      path: "pipeline/setup-docker.sh",
      privileged: false
    s.vm.provision :shell,
      path: "pipeline/setup-minikube.sh",
      privileged: false
    s.vm.provision :shell,
      path: "pipeline/setup-kube.sh",
      privileged: false
    s.vm.provision :shell,
      path: "pipeline/setup-helm.sh",
      privileged: false
    s.vm.provision :shell,
      inline: "sudo apt-get install -y openjdk-8-jdk",
      privileged: false
    s.vm.provider "virtualbox" do |v|
      v.name = "integration"
      v.cpus = 2
      v.memory = 7168
      v.gui = false
      v.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
      #v.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
    end
  end
end
