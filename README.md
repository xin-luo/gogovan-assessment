## Gogovan Assignment by Xin Luo

### Overview
Following the instructions here https://gist.github.com/dg3feiko/1f918cf03dd38a8529f3ca35bd4b3f06 I have created an implementation of
the requested application. As explained in my video, I set various configuration options in application.properties so that this
program can simulate various situations and see how they affect the metrics being generated.

As for the tech, the events are sent to Kinesis using AWS's Kinesis Consumer and Producer Libaries. I have only tested this locally
using Kinesalite. My understanding is that there's newer versions of the above libraries available, but Kinesalite is unable to 
support them as they require implementing additional features (e.g. Consumer fan-out). I am not sure if these older libraries would
still work on an actual AWS endpoint. DynamoDB (via the local dynalite process) is used by their Consumer Library to keep track of
the current state of each consumer, e.g. how many events it has consumed from each shard.

### Installing the application
This is a Java Maven based application and can be built using `mvn clean install`. Once the shaded uberjar which is the standalone
version of the application is built, you can use the `run.bat` or `run.sh` commands, or execute the command directly:
`java -Dcom.amazonaws.sdk.disableCertChecking -jar target/assignment-1.0-SNAPSHOT.jar`. The `disableCertChecking` option is
necessary for running Kinesalite locally to prevent the application from checking for valid security certificates.

### Running the application
If running locally, the application assumes you have `kinesalite` and `dynalite` already installed and available in your PATH variable. 
Those can be installed using `npm`, with `npm install -g kinesalite` for example. 

By default, the application will make several endpoints available on http://localhost:8080, namely one for each metrics provider.
The home page is an html page that uses Javascript/Angular to read each API endpoint sending Server Side Events and updates its
respective table accordingly. You can run the application with custom configs by putting your own `application.properties` file
in the same folder as the `.jar` file.

### Testing
I wrote a few simple tests for the application, but they are pretty sparse due to the time limit given. I have green path tests
for the metrics providers, as well as the event producers/consumers. With additional time, I'd have written some red path tests
to see how the application behaves when given unusual/out of bounds inputs, as well as some tests on the Kinesis producer and
processor classes. Lastly, I would include some integration tests involving the REST endpoints and making sure the metrics are
being sent properly.

### Video Links
Explaining the code: https://www.youtube.com/watch?v=3_Vud8tRF5Y&feature=youtu.be

Demoing the application: https://www.youtube.com/watch?v=hkcz5tSg8ic&feature=youtu.be
