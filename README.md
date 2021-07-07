# Certified DAG Template
### This template is meant to standardize the repository structure and provide customized files where necessary for Certified DAGs which will be surfaced on the Astronomer Registry.  

Follow the steps below to use the template and initialize a new repository locally.

1. Above the file list, click **Use this template**

![image](https://user-images.githubusercontent.com/48934154/122494828-a8a0b000-cfb7-11eb-8d51-5fb4aa47a32f.png)

2. Using the Owner drop-down menu, select **astronomer** to own the repository

![image](https://user-images.githubusercontent.com/48934154/122494551-94f54980-cfb7-11eb-8962-bd3333fde6e1.png)

3. Type a name for the repository, and an optional description.

![image](https://user-images.githubusercontent.com/48934154/122496102-35983900-cfb9-11eb-8074-ccd5b9529d8d.png)

4. Select **Public** visibility for the repository.

![image](https://user-images.githubusercontent.com/48934154/122496127-3f21a100-cfb9-11eb-8540-48f53c1b7d9c.png)

5. Click **Create repository from template**.

6. Clone the repository locally. Refer to the GitHub [documentation](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository-from-github/cloning-a-repository) for different options of cloning repositories.

7. [Optional] Navigate to where the repository was cloned and run the following Astro CLI command to update the project.name setting in the .astro/config.yaml file provided in the repository.  This will update the name used to generate Docker containers and make them more discernible if there are multiple Certified DAGs initialized locally. 
```bash
astro config set project.name <name of repository>
```
8. To beginning development and testing of the Certified DAG, run `astro dev start` to spin up a local Airflow environment. There is no need to run `astro dev init` as this functionality is already built in to the template repository.
