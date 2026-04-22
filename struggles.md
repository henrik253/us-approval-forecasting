
# Struggles during the project
- Problems with IAM -> high learning curve 
- Struggles with AWS Application to github to build the project when a new push happens 
- Libraries need to be zipped in AWS Lambda with a limit of 50mb in the Lambda enviroment. Libs like numpy and numpy exceed this limit and also are unneccessary.
- handler.py returned the data which AWS Lambda has a limit of. BTW even though claude knows the context of all of this bugs still happen easily! 
- Executing a notebook from a worksheet, took me 2 hours.
- Jinja code cant be exectued in snowflake by using RUN button  
- Uploading files to a own created stage, in order to execute them from other files. 
- GDELT API will either block your response or give you incosistent responses 
- Used LLMs like Gemini or GPT which hallucinate extreme when it comes to nieche snowflake knowledge