# RedCrossQuestCloudFunctions

Cloud functions for RedCrossQuest/RedQuest


how to get code completion per function without having a dedicated node_modules folder per function

* create a 'node_modules' folder at the root of this project
* run 'create_node_modules_links.sh' that will create a link from the function folder to the root folder
* a 'virtual' package.json is at the root of the folder. in this package, cumulate all de dependencies of the functions.
  * you add a dependency in a function, add it also in the root package.json
  * run npm install on the **root package.json**, not on the function package.json, 
    otherwise, all unnecessary dependencies for this function will be removed
   
* with the links, you'll get code completion for all your libs



