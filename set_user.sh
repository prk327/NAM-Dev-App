#!/bin/bash

# Set the user ID and group ID to match the host system
USER_ID=${LOCAL_UID:-1000}
GROUP_ID=${LOCAL_GID:-1000}

# Create a new group and user based on the host user's ID
groupadd -g $GROUP_ID usergroup
useradd -u $USER_ID -g usergroup -m -s /bin/bash user

# Give the new user sudo permissions
echo "user ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Change the ownership of the working directory
chown -R user:usergroup /home/jovyan/work

# Switch to the newly created user
su user

# Start Jupyter
exec jupyter notebook --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token=''