### v1.4.0 - Apr 23, 2017
- contributed by @wolfgang-s
  - add support for ES 5.x (PR #87 and PR #89)

### v1.3.5 - March 7, 2017
- Fixed Issue - Remove refresh option for unsupported methods
  - This issue was fixed in PR #86
  
### v1.3.4 - Feb 25, 2017
- Fixed Issue #79  - Wait until the document is ready for searching
  - this issue was fixed in PR #81
- Fixed Issue #85  - Update All for es2.3 and above
  - this issue was fixed in PR #84
    
### v1.3.3 - Feb 23, 2017
- Fixed Issue #79  - Multiple where clause filter support without `and` filter
  - this issue was fixed in PR #78
- Fixed Issue #73  - Nested and, or filter support added
  - this issue was fixed in PR #75
- Fixed Issue #73  - `inq`,`nin`,`between`,`neq` filter support added
  - this issue was fixed in PR #75
- Fixed Issue #28 - Include filter added
  - Minimum workflow to make include filter work until we optimize it for better performance.
    This issue was fixed in PR #71
    
### v1.3.2 - Jan 31, 2017
- Fixed Issue #64  - Date strings were not returned as Javascript Data objects
  - this issue was fixed in PR #68
- Fixed Issue #37 - MakeId refactoring applied for save method
  - Save method still had an old implementation of makeId method which was updated with
    getDocumentId method. This issue was fixed in PR #69
- Fixed Issue - Objects inside an array were returned as strings
  - This issue was fixed in PR #70

### v1.3.1 - Dec 02, 2016
- Fixed Regression - analyzers for index weren't being created anymore
  - originally this was added in #25 and got lost somewhere along the way

### v1.3.0 - Oct 17, 2016
- contributed by @dtomasi
  - Issue #57: Fix for defaultSize setting
  - Issue #55: Support Signed Requests for AWS Elasticsearch-Service

### v1.2.0 - Sep 25, 2016
- Add eslint infrastructure
  - `npm install --save-dev eslint@2.13.1`
    - ESLint v3.0.0 now requires Node.js 4 or higher. If you still need ESLint to run on Node.js < 4, then we recommend staying with ESLint v2.13.1 until you are ready to upgrade your Node.js version.
  - https://github.com/strongloop/loopback-contributor-docs/blob/master/eslint-guide.md
  - https://github.com/strongloop/eslint-config-loopback

### v1.1.0 - Sep 25, 2016
- Fixed Issue #52
  - Multi Index usage
    - any model specific indices or mappings should be setup when the connector is initialized
    - mappings array in datasource.<env>.json may contain the index and type properties, which will be used to setup that model's index and mappings during connector initialization

### v1.0.8 - Sep 21, 2016
- Fixed Issue #51

### v1.0.7 - Aug 11, 2016
- Fixed Issue #45
