# CDC Salesforce Streaming Source

Description
-----------
This plugin reads Change Data Capture (CDC) events from Salesforce.

All CDC source plugins are normally used in conjunction with CDC sink plugins. 
CDC source produces messages in CDC format. 

Properties
----------
**Client Id**: Client ID from the connected app.

**Client Secret**: Client Secret from the connected app.

**Username**: Username to use when connecting to Salesforce.

**Password**: Password to use when connecting to Salesforce.

**Login Url**: Salesforce login URL to authenticate against. 
The default value is https://login.salesforce.com/services/oauth2/token. 
This should be changed when running against the Salesforce sandbox.

**Tracking Objects**: Objects to read change events from (For example: Task for base object and Employee__c for custom) separated by ",".
If list is empty then subscription for all events will be used.

**Error Handling**: Possible values are: "Skip on error" or "Fail on error". These are strategies on handling records 
which cannot be transformed. "Skip on error" - just skip, "Fail on error" - fails the pipeline if at least one erroneous 
record is found.

Note: CDC must be enabled on the database for the source to read the change data.

Salesforce Change Data Capture
--------------------------
When something changes in object for which is enable 'Change notifications'. A Change Data Capture event, or change 
event, is a notification that Salesforce sends when a change to a Salesforce record occurs as part of a create, update, 
delete, or undelete operation. The notification includes all new and changed fields, and header fields that contain 
information about the change. For example, header fields indicate the type of 
change that triggered the event and the origin of the change. Change events support all custom objects and a subset of 
standard objects. More information can be found in  [official documentation](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_intro.htm).

### Enable Change Data Capture for objects
To enable Change Data Capture for objects in Salesforce you have to 
[select Objects for Change Notifications](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_select_objects.htm)