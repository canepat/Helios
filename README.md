# Helios
**Helios** is a component library for domain-specific scalable applications based on [Aeron](https://github.com/real-logic/Aeron) and [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor).

Its goal is providing flexible and lightweight abstract data types for scalable architectures composed by protocol-layer Gateways and business-layer Services, exploiting some state-of-the-art techniques for journalling, replicating and archiving protocol messages. This work is greatly inspired by publicly available video talks and papers about Aeron and LMAX Disruptor architectures.

For usage description, FAQ, etc please check out the [Wiki](https://github.com/canepat/Helios/wiki)

For the latest version information and changes please see the [Change Log](https://github.com/canepat/Helios/wiki/Change-Log)

License (see LICENSE file for full disclaimer)
----------------------------------------------

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


Build
-----

You require the following to build Helios:

* Latest stable [Open JDK 8](http://openjdk.java.net/projects/jdk8/) or [Oracle JDK 8](http://www.oracle.com/technetwork/java/)
* Latest stable [Gradle](http://gradle.org/getting-started-gradle-java/)

Full clean and build of the project:

    $ ./gradlew
    

