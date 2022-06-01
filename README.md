# Introduction
This is a POC code that we are experimenting with to help improve our storage performance.  As such, the code here is quite messy.

# High level explanation
In short, it splits the file into sectors of 1MB each.  Each sector is either a part of the hasthtable, a chunk of values, or a delmap.  Hashtable is open-addressed mapping from hashes of the keys (I assume hashes never collide) into offsets into the values.  Each chunk of values is just contiguous values, and a delmap stores one bit per 128 bytes of values indicating whether the corresponding value is still present, or needs to be garbage collected.  GC happens by treating the values as a queue, and moving the front four values to the back every time you delete one value (or, correspondingly, discarding them for good if they are scheduled for being GCed).
One can prove that with this scheme the number of values scheduled for GC never exceeds the number of values that are actually still there.  The logic of the hashtable is in the methods with prefix ht_, and is tested fuzzily in test_fuzzy_db_ht_consistency.  The logic for the entire thing is tested by test_fuzzy_storage_consistency, which is also a good test to look at the interface, and how to use it.
The latter also ocassionally recreates the hashtable, thus testing recovery from disk.
