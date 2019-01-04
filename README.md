# MalML (Malware Machine Learning)

![alt text][pipeline]

**MalML** is a framework for extracting malware features from a sandbox and building and evaluating ML model performance results. This was submitted in partial fulfilment of the requirements of Edinburgh Napier University for the Degree of **MSc Advanced Security and Digital Forensics**.

# Dissertation experimentation

![alt text][dissertation-chunking-exp]
The features used for experimentation in the dissertation project revolved around examining low level memory access patterns. More specifically, separating the address space into 1000 byte chunks and looking at relative target memory access distances within each chunk. This specific featre selection was inspired by **(Ozsoy, Donovick, Gorelik, Abu-Ghazaleh, & Ponomarev, 2015)**.

Data sources included samples from theZoo malware repository, manually selected samples from VirusTotal and benign samples from fresh Windows installs and Chocolatey (Windows package manager).

Sample results showing evaluation of Decision Tree and SVM classifiers:
![alt text][sample-results]

# Technologies
- VirtualBox
- Intel PinTool, C Programming Language
- Python, Pyspark, MLLib, Flask
- Redis
- ElasticSearch

# Design requirements
- **Performant experimentation at scale**: This was accomplished through micro-services and asynchronous communication. This allowed different stages to run in parallel (sample detonation, feature extraction, model generation, model evaluation).
- **Support for multiple architectures**: - This was enabled through the abstraction of extractor packs, which allowed different processes per sandbox machine type (32bit/64bit). 

# High level architecture
Selection of sample, detonation in a VM with a specific "extractor pack", capture results:
![alt text][detonation]
Identify feature extractor plugin for the output from the specific and generate the feature data ready for input to ML models:
![alt text][feature-extraction]
Poll for unprocessed feature data and use this to generate different ML models:
![alt text][model-generation]

# Sandbox
VirtualBox windows machines were configured to run a Python stub at start up, this fetches the sandbox agent code responsible for detonation of samples and returning results to the controller:
![alt text][sandbox-highlevel]
Extractor packs were designed to allow the framework to work across different architectures and operating systems. The example below shows a Windows package, containing the 32bit runtime for Intel PinTool and compiled pins: 
![alt text][extractor-pack]
The manifest file inside a pack instructs the sandbox agent what to execute and which results to capture and upload back to the controller:
![alt text][manifest]

# Directory overview
| directory        | contents                                                                       |
|------------------|--------------------------------------------------------------------------------|
| data/            | Placeholder directory to capture data from services                            |
| extractor-packs/ | Placeholder directory for extractor pack storage                               |
| guest-agent/     | The stage2 sandbox agent which is sent to virtual machine                      |
| guest-stub/      | The stage1 sandbox loader which is run in virtual machines at boot             |
| http-controller/ | Responsible for orchestrating sandbox detonations and receiving results        |
| model-gen/       | Daemon to retrieve extracted features and build different ML models            |
| sample-mgr/      | Parse and prioritise samples on disk for detonation                            |
| tools/           | misc tools                                                                     |
| vbox-controller/ | Scripts for start/restart/restore to snapshot for VirtualBox                   |
| vm_watchdog/     | Daemon to orchestrate sandbox machines and to monitor and recover from crashes |

# Previous work
This piece of work was significant in size, so three independent Proof-Of-Concept (POC) implementations were created prior to starting, to assess the feasibility of this project.

The first was a POC to perform feature extraction, ML model generation and to evaluate the performance of these using features selected from static analysis of PE binaries: https://github.com/Tiergarten/ml-static-poc

The second POC leveraged Intel PinTool to extract low level memory access patterns and Python code to transform these into features: https://github.com/Tiergarten/e2e-binextract-fextract-poc. The code from this POC ultimately ended up being the functional contents of the extractor pack used for experimentation.

A third POC evaluated the effect which different normalisation techniques applied on the feature data has on model performance: https://github.com/Tiergarten/feature-normalisation-poc.

# Future work
- Look to leverage AWS for efficiencies. One avenue would be EC2 for compute, another would be to leverage Lambda services for reduced running time.

# References
- Ozsoy, M., Donovick, C., Gorelik, I., Abu-Ghazaleh, N. B., & Ponomarev, D. V. (2015). Malware-aware processors: A framework for efficient online mal- ware detection. In HPCA (pp. 651–661). IEEE Computer Society. http://www.cs.binghamton.edu/~secarch/hpca15.pdf

[pipeline]: https://s3.amazonaws.com/overcooked.juggernaut/malml-images/malml-pipeline+(1).png "MalML Pipeline"

[dissertation-chunking-exp]: https://s3.amazonaws.com/overcooked.juggernaut/malml-images/reference_chunking+(1).png

[sandbox-highlevel]: https://s3.amazonaws.com/overcooked.juggernaut/malml-images/sandbox_implementation+(1).png

[detonation]: https://s3.amazonaws.com/overcooked.juggernaut/malml-images/detonator_implementation+(1).png

[feature-extraction]:https://s3.amazonaws.com/overcooked.juggernaut/malml-images/detonation-extraction+(2).png

[model-generation]:https://s3.amazonaws.com/overcooked.juggernaut/malml-images/model_generation_implementation+(1).png

[extractor-pack]:https://s3.amazonaws.com/overcooked.juggernaut/malml-images/extractor_pack+(1).png

[manifest]:https://s3.amazonaws.com/overcooked.juggernaut/malml-images/manifest-screenshot.PNG

[sample-results]: https://s3.amazonaws.com/overcooked.juggernaut/malml-images/Screenshot+2019-01-04+at+11.37.47.png
