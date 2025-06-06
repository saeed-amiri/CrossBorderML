SELECT name 
  FROM sqlite_master 
 WHERE type='table' 
   AND name GLOB '*_wide';