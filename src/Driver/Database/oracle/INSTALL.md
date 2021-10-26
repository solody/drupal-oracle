# Driver installation.

There are two ways to install Drupal 8 Oracle Database Driver.

## 1. Composer way (the preferred one).
Step 1. Add one more installer-paths to yours composer.json:  
`"web/drivers/lib/Drupal/Driver/Database/{$name}": ["drupal/oracle"]`  
This path should be added before "type:drupal-module" path.  
More details about the issue with installer-paths:  
https://www.drupal.org/node/2924316

Step 2. Add needed patches to "extra" > "patches" section in your composer.json:  
```
"extra": {
    "patches": {
        "drupal/core": {
            "Log::findCaller fails to report the correct caller function with non-core drivers": "https://www.drupal.org/files/issues/2019-05-28/2867788-53.patch",
            "non-standard precision limits at testSchemaAddFieldDefaultInitial": "https://www.drupal.org/files/issues/2020-01-28/drupal-3109651-SchemaTest_precision_limits-3.patch"
        }
    }
},
```

Step 3. Run the `composer` commands as usually:  
`composer require drupal/oracle`  
`composer install`


## 2. Manual way.

Download an archive from drupal.org project page and place the code in this
directory: `DRUPAL_ROOT/drivers/lib/Drupal/Driver/Database/oracle`

Apply all needed patches to the Drupal core and tests run:
 - https://www.drupal.org/files/issues/2019-05-28/2867788-53.patch
 - https://www.drupal.org/files/issues/2020-01-28/drupal-3109651-SchemaTest_precision_limits-3.patch
