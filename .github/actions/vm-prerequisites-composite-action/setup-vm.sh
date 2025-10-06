#!/bin/bash
set -e

# Get the required versions from arguments
REQUIRED_MAVEN_VERSION="$1"
REQUIRED_DOCKER_VERSION="$2"

echo "Required Maven version: ${REQUIRED_MAVEN_VERSION}"
echo "Required Docker version: ${REQUIRED_DOCKER_VERSION}"

# Install Git if needed
if ! command -v git &> /dev/null; then
  echo 'Installing Git...'
  sudo apt-get update
  sudo apt-get install -y git
else
  echo 'Git is already installed'
fi

# Check for Java
if ! command -v java &> /dev/null || ! java -version 2>&1 | grep -q 'version \"17'; then
  echo 'Installing JDK 17...'
  sudo apt-get update
  sudo apt-get install -y openjdk-17-jdk
else
  echo 'JDK 17 is already installed'
fi

# Define helper function for version comparison
version_to_number() {
  local version=$1
  # Extract major, minor, patch - handling missing components
  local major=$(echo $version | cut -d. -f1)
  local minor=$(echo $version | cut -d. -f2 2>/dev/null || echo 0)
  local patch=$(echo $version | cut -d. -f3 2>/dev/null || echo 0)

  # Print as comparable number with zero padding
  printf "%d%03d%03d" "$major" "$minor" "$patch"
}

REQ_VERSION_INT=$(version_to_number "$REQUIRED_MAVEN_VERSION")
echo "Required Maven numeric: $REQ_VERSION_INT"

# Remove any existing broken Maven installation if present
if dpkg -l | grep -q maven && ! command -v mvn &> /dev/null; then
  echo "Found broken Maven installation. Removing it..."
  sudo apt-get remove -y maven
  sudo apt-get autoremove -y
fi

# Check existing Maven version
if command -v mvn &> /dev/null; then
  MVN_VERSION=$(mvn --version 2>/dev/null | head -n 1 | awk '{print $3}' || echo 'unknown')
  echo "Current Maven version: $MVN_VERSION"

  # Check if current version meets requirements
  if [ "$MVN_VERSION" != "unknown" ]; then
    MVN_VERSION_INT=$(version_to_number "$MVN_VERSION")
    echo "Current Maven numeric: $MVN_VERSION_INT, Required: $REQ_VERSION_INT"

    if [ $MVN_VERSION_INT -ge $REQ_VERSION_INT ]; then
      echo "Current Maven $MVN_VERSION meets requirement"
      echo "Maven binary location: $(which mvn)"
    else
      echo "Current Maven $MVN_VERSION does not meet requirement $REQUIRED_MAVEN_VERSION"
      # Need to install newer Maven
      sudo rm -f $(which mvn)
    fi
  fi
else
  echo "Maven command not found"
fi

# If mvn command doesn't exist or version wasn't adequate, install Maven
if ! command -v mvn &> /dev/null; then
  echo "Installing Maven from Apache website..."
  sudo mkdir -p /opt/maven
  cd /tmp

  # Try several known versions from newest to oldest
  for VERSION in '3.9.8' '3.9.5' '3.9.4' '3.9.3' '3.9.2' '3.9.1' '3.9.0'; do
    echo "Trying to install Maven $VERSION..."
    wget -q https://archive.apache.org/dist/maven/maven-3/$VERSION/binaries/apache-maven-$VERSION-bin.tar.gz

    if [ -f apache-maven-$VERSION-bin.tar.gz ]; then
      echo "Downloaded Maven $VERSION successfully"
      sudo tar xzf apache-maven-$VERSION-bin.tar.gz -C /opt/maven
      sudo ln -sf /opt/maven/apache-maven-$VERSION/bin/mvn /usr/local/bin/mvn

      # Verify installation
      if command -v mvn &> /dev/null; then
        echo "Maven $VERSION installed successfully:"
        mvn --version
        echo "Maven location: $(which mvn)"
        break  # Success!
      else
        echo "Maven binary not found in path after installation"
        # Try creating symlink in another location
        sudo ln -sf /opt/maven/apache-maven-$VERSION/bin/mvn /usr/bin/mvn
        if command -v mvn &> /dev/null; then
          echo "Maven $VERSION installed successfully after symlink fix:"
          mvn --version
          break  # Success!
        fi
      fi
    else
      echo "Failed to download Maven $VERSION, trying next version..."
    fi
  done

  # Last resort: try apt installation
  if ! command -v mvn &> /dev/null; then
    echo "All Apache downloads failed. Trying apt as last resort..."
    sudo apt-get update
    sudo apt-get install -y maven

    # Verify installation again
    if command -v mvn &> /dev/null; then
      echo "Maven installed via apt:"
      mvn --version
    else
      # Try to find the maven binary anywhere on the system
      echo "Maven command still not available. Searching for maven binary..."
      MAVEN_BIN=$(find /usr -name mvn 2>/dev/null | head -1 || echo "")

      if [ -n "$MAVEN_BIN" ]; then
        echo "Found Maven binary at $MAVEN_BIN"
        sudo ln -sf $MAVEN_BIN /usr/local/bin/mvn
        echo "Created symlink to: /usr/local/bin/mvn"
        mvn --version
      else
        echo "FATAL: Could not find Maven installation anywhere"
        exit 1
      fi
    fi
  fi
fi

# Check for Docker and install if needed
echo "Checking Docker installation..."

# Check if Docker is already installed and running
DOCKER_RUNNING=false
if command -v docker &> /dev/null; then
  DOCKER_VERSION=$(docker --version 2>/dev/null | awk '{print $3}' | sed 's/,//' || echo 'unknown')
  echo "Docker version found: $DOCKER_VERSION"

  # Check Docker version meets requirements
  if [ "$DOCKER_VERSION" != "unknown" ]; then
    # Remove leading 'v' if present in version string
    DOCKER_VERSION=${DOCKER_VERSION#v}
    DOCKER_VERSION_INT=$(version_to_number "$DOCKER_VERSION")
    REQUIRED_DOCKER_INT=$(version_to_number "$REQUIRED_DOCKER_VERSION")

    echo "Docker version numeric: $DOCKER_VERSION_INT, Required: $REQUIRED_DOCKER_INT"

    if [ $DOCKER_VERSION_INT -ge $REQUIRED_DOCKER_INT ]; then
      echo "Docker version $DOCKER_VERSION meets requirement"

      # Check if Docker daemon is running
      if sudo docker info &> /dev/null; then
        echo "Docker daemon is running."
        DOCKER_RUNNING=true
      else
        echo "Docker is installed but daemon is not running."
      fi
    else
      echo "Docker version $DOCKER_VERSION does not meet minimum requirement $REQUIRED_DOCKER_VERSION"
    fi
  fi
else
  echo "Docker is not installed"
fi

# Install or start Docker if needed
if [ "$DOCKER_RUNNING" = false ]; then
  echo "Installing Docker for Debian..."

  # Install prerequisites
  sudo apt-get update
  sudo apt-get install -y ca-certificates curl gnupg

  # Set up Docker's apt repository - Debian specific
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg

  # Add the repository to Apt sources
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

  # Install Docker packages
  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

  # Start and enable Docker service
  sudo systemctl enable docker
  sudo systemctl start docker

  # Add current user to docker group
  sudo usermod -aG docker $USER
  echo "Docker installed. You may need to log out and back in for group changes to take effect"

  # Verify Docker installation and daemon
  if sudo docker info &> /dev/null; then
    echo "Docker installation verified and daemon is running."
  else
    echo "Docker daemon not running. Attempting to start it..."
    sudo systemctl restart docker
    sleep 5

    if sudo docker info &> /dev/null; then
      echo "Docker daemon started successfully after restart."
    else
      echo "WARNING: Docker daemon still not running. Tests that require Docker may fail"
    fi
  fi
fi

# Test Docker with a simple hello-world container
echo "Testing Docker with hello-world container..."
sudo docker run --rm hello-world &> /tmp/docker_test_output.txt || true

if grep -q "Hello from Docker!" /tmp/docker_test_output.txt; then
  echo "Docker is working correctly!"
  cat /tmp/docker_test_output.txt
else
  echo "Docker test failed. Output:"
  cat /tmp/docker_test_output.txt
  echo "WARNING: Docker may not be functioning correctly. Tests requiring Docker may fail"
fi