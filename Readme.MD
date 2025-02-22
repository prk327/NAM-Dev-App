Here's how to connect two Docker containers (Vertica and the Python container) by creating and linking them to a custom network:

Steps to Create and Link Docker Networks for Communication Between Two Containers
1. Create a Custom Docker Network
First, create a custom bridge network so that the containers can communicate with each other.

```properties
docker network create vertica-network
```
This command will create a network named vertica-network.

2. Attach Vertica Container to the Custom Network
Next, you need to attach the already running Vertica container to the custom network you just created. You can do this using the docker network connect command.

First, identify your Vertica container's name or ID by running:

```properties
docker ps
```
Then, connect the Vertica container to the vertica-network:

```properties
bash - docker network connect vertica-network <vertica_container_name_or_id>
```
Replace ```<vertica_container_name_or_id>``` with the actual name or ID of your Vertica container. This allows the Vertica container to communicate with other containers on the same network.

3. Run the Python Container on the Same Network
Now, when you run your Python container, ensure it’s connected to the same custom network. Here's how to run the Python container and attach it to the same network as Vertica:

```properties
bash - docker run -d --name python-script-container --network vertica-network my-python-image
```
```--network vertica-network:``` This ensures the Python container joins the vertica-network, allowing it to communicate with Vertica.
Replace my-python-image with the name of the image you're using to run the Python script.

4. For Docker Compose add the below in service:

    ```properties
    services:
        jupyter:
        ...
            networks:
                - vertica-network
    
    networks:
        vertica-network:
            external: true               # Use an external network if Vertica is outside Compose
    ```

5. To connect to vertica from python script, inspect

```properties
docker inspect vertica-network
```

look for Containers key and find the name that you can use to connect to vertica under host

Something like below 
```
"Containers": {
            ".....": {
                "Name": "vertica-ce",
                "EndpointID": "...",
                "MacAddress": "...",
                "IPv4Address": ".../...",
                "IPv6Address": ""
            }}
```