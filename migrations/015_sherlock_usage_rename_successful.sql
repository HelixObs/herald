-- "successful" was really tracking whether the turn ended in a
-- submit_hypothesis conclusion — true for a structured investigation
-- reaching a verdict, false otherwise. Once general chat started running
-- through the same code path, every plain chat message (which never calls
-- submit_hypothesis) landed here as "successful = false" even when nothing
-- went wrong, which reads as an error. Renamed to what it actually means.

ALTER TABLE sherlock_usage RENAME COLUMN successful TO reached_hypothesis;
