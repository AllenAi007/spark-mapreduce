One of classic map and reduce case we might need to use during our work
1. Input:
h,masterId,branchId,branchName
d,1,1,Chengdu
d,1,2,Chongqing
d,2,1,Shanghai
d,2,2,Beijing
d,2,3,Guangzhou
d,2,4,Shenzheng
d,3,1,Other
2. Ouput:
h,masterId,branchId,branchName
d,1,1||2,Chengdu||Chongqing
d,1,2,Chongqing
d,2,1||2||3||4,Shanghai||Beijing||Guangzhou||Shenzheng
d,3,1,Other