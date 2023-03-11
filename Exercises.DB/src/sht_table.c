#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int hash(char* name, int buckets);

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
    int fd;
    BF_Block* block;
    char* data=NULL;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_CreateFile(sfileName));
    CALL_OR_DIE(BF_OpenFile(sfileName, &fd));
    // Create block 0
    CALL_OR_DIE(BF_AllocateBlock(fd, block));
    data = BF_Block_GetData(block);
    memset(data,0,BF_BLOCK_SIZE);
    memcpy(data, &buckets, sizeof(buckets));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    // Create the other blocks
    for (int i = 1; i <= buckets; i++) {
        CALL_OR_DIE(BF_AllocateBlock(fd, block));
        data = BF_Block_GetData(block);
        memset(data, 0, BF_BLOCK_SIZE);
        int next_block_id = -1;
        int pairsnum=0;
        memcpy(data, &next_block_id, sizeof(int));
        memcpy(data+sizeof(int),&pairsnum,sizeof(int));
        BF_Block_SetDirty(block);
        CALL_OR_DIE(BF_UnpinBlock(block));
    }
     BF_Block_Destroy(&block);
    CALL_OR_DIE(BF_CloseFile(fd));

    return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
    SHT_info *info;
    int fd;
    BF_Block *block;
    char *data;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_OpenFile(indexName, &fd));
    info = malloc(sizeof(SHT_info));
    info->fileDesc = fd;

    CALL_OR_DIE(BF_GetBlock(fd, 0, block));
    data = BF_Block_GetData(block);
    memcpy(&info->buckets, data, sizeof(int));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    return info;
}


int SHT_CloseSecondaryIndex( SHT_info* SsHT_info ){
    int fd;

    if (SsHT_info) {
        fd = SsHT_info->fileDesc;
        free(SsHT_info);
        CALL_OR_DIE(BF_CloseFile(fd));

        return 0;
    }
    return -1;

}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
    Pair pair;
    memcpy(pair.name, record.name, strlen(record.name) + 1);
    pair.blocknum = block_id;
    int fd, buckets, bucket_id;
    BF_Block *block;
    char *data;
    char *data2;
    int next_block_id;
    BF_Block_Init(&block);
    fd = sht_info->fileDesc;
    buckets = sht_info->buckets;
    // Find the block that corresponds to the bucket
    CALL_OR_DIE(BF_GetBlock(fd,  hash(record.name, sht_info->buckets), block));
    data = BF_Block_GetData(block);
    memcpy(&next_block_id, data, sizeof(int));
    // Find the last block in the bucket with space to insert the record
    while (next_block_id != -1) {
        CALL_OR_DIE(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
        BF_Block_Init(&block);
        CALL_OR_DIE(BF_GetBlock(fd, next_block_id, block));
        data = BF_Block_GetData(block);
        memcpy(&next_block_id, data, sizeof(int));
    }
    // Check if there is space in the current block to insert the record
    int pairsnum=0;
    data+=sizeof(int);

    int blocksnumbers;
    memcpy(&pairsnum,data,sizeof(int));
    if (((pairsnum+1)*sizeof(Pair) + sizeof(char)+(2*sizeof(int))) < BF_BLOCK_SIZE) {
        // There is space, insert the record and exit the loop
        
        pairsnum++;
        memcpy(data,&pairsnum,sizeof(int));
        data+=sizeof(int) +(pairsnum-1)*sizeof(Pair);
        memset(data,0,sizeof(int));
        memcpy(data, &pair.blocknum,sizeof(int));
        int papari;
        memcpy(&papari,data,sizeof(int));

        data += sizeof(int);


        memset(data,0,15*sizeof(char));
        memcpy(data , pair.name , strlen(pair.name)+1);
        data-=sizeof(int);

    } else {
        // No space
        // Allocate a new block and add it to the end of the bucket
        CALL_OR_DIE(BF_AllocateBlock(fd, block));
        data2 = BF_Block_GetData(block);
        memset(data2, 0, BF_BLOCK_SIZE);
        next_block_id=-1;
        memcpy(data2,&next_block_id,sizeof(int));

        pairsnum =1;
        data2+=sizeof(int);
        memset(data2,0,sizeof(int));
        memcpy(data2, &pairsnum, sizeof(int));

        data2+=sizeof(int);
        memset(data2,0,sizeof(int));
        memcpy(data2, &pair.blocknum,sizeof(int));

        data2 += sizeof(int);
        memset(data2,0,15*sizeof(char));
        memcpy(data2 , pair.name , strlen(pair.name)+1);

        data2-=sizeof(int);

        
        CALL_OR_DIE(BF_GetBlockCounter(fd,&blocksnumbers));

        data-=sizeof(int);
        memset(data,0,sizeof(int));
        blocksnumbers--;
        memcpy(data,&blocksnumbers,sizeof(int));

        BF_Block_SetDirty(block);
        CALL_OR_DIE(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
    }
    return 0;
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
    int blocknum;
    int id;
    char sname[15];
    char surname[20];
    char city[20];
    char *dataht;
    int fd, buckets, block_id;
    BF_Block *block;
    char *data;
    int next_block_id;
    Pair pair;
    int count=0;
    BF_Block_Init(&block);
    fd = sht_info->fileDesc;
    buckets = sht_info->buckets;
    int i;
    for (i = 0; i < buckets; i++) {
        // Find the block that corresponds to the bucket
        block_id = i + 1;
        CALL_OR_DIE(BF_GetBlock(fd, block_id, block));
        data = BF_Block_GetData(block);
        memcpy(&next_block_id, data, sizeof(int));
        // Iterate over the blocks in the bucket
        while (next_block_id != -1){
            // Get the record
            data = BF_Block_GetData(block);
            data+=sizeof(int);
            int recordsnum;
            memcpy(&recordsnum, data, sizeof(int));
            data+=sizeof(int);
            int j;
            for(j=0;j<recordsnum;j++){
                memcpy(&blocknum,data+j*sizeof(Pair),sizeof(int));
                memcpy(&sname,data+(j*sizeof(Pair)+sizeof(int)),15*sizeof(char));
                if(strcmp(name,sname))
                    continue;
                BF_Block *htblock;
                BF_Block_Init(&htblock);

                if (BF_GetBlock(ht_info->fileDesc,blocknum, htblock) != BF_OK){
                    return -1;
                }

                dataht = BF_Block_GetData(htblock);
                dataht+=sizeof(int);
                int recordsnum;
                memcpy(&recordsnum, dataht, sizeof(int));
                dataht+=2*sizeof(int);
                int z;
                for(z=0;z<recordsnum;z++){
                    memcpy(&id,dataht+z*sizeof(Record),sizeof(int));
                    memcpy(&sname,dataht+(z*sizeof(Record)+sizeof(int)),15*sizeof(char));
                    memcpy(&surname,dataht+(z*sizeof(Record)+15*sizeof(char)+sizeof(int)),20*sizeof(char));
                    memcpy(&city,dataht+(z*sizeof(Record)+20*sizeof(char)+15*sizeof(char)+sizeof(int)),20*sizeof(char));
                    if(strcmp(name,sname))
                        continue;
                    printf("id %d name %s surname %s city %s\n",id,name,surname,city);
                }
            }
            // Find the next block in the bucket
            CALL_OR_DIE(BF_UnpinBlock(block));
            CALL_OR_DIE(BF_GetBlock(fd, next_block_id, block));
            data = BF_Block_GetData(block);
            memcpy(&next_block_id, data, sizeof(int));
            count++;
        }
        data+=sizeof(int);
        int recordsnum;
        memcpy(&recordsnum, data, sizeof(int));
        data+=sizeof(int);
        int j;

        for(j=0;j<recordsnum;j++){
              
                memcpy(&blocknum,data+j*sizeof(Pair),sizeof(int));
                memcpy(&sname,data+(j*sizeof(Pair)+sizeof(int)),15*sizeof(char));
                if(strcmp(name,sname))
                    continue;
                BF_Block *htblock;
                BF_Block_Init(&htblock);

                if (BF_GetBlock(ht_info->fileDesc,blocknum, htblock) != BF_OK){
                    return -1;
                }

                dataht = BF_Block_GetData(htblock);
                dataht+=sizeof(int);
                int recordsnum;
                memcpy(&recordsnum, dataht, sizeof(int));
                dataht+=sizeof(int);
                int z;
                for(z=0;z<recordsnum;z++){
                    memcpy(&id,dataht+z*sizeof(Record),sizeof(int));
                    memcpy(&sname,dataht+(z*sizeof(Record)+sizeof(int)),15*sizeof(char));
                    memcpy(&surname,dataht+(z*sizeof(Record)+15*sizeof(char)+sizeof(int)),20*sizeof(char));
                    memcpy(&city,dataht+(z*sizeof(Record)+20*sizeof(char)+15*sizeof(char)+sizeof(int)),20*sizeof(char));
                    if(strcmp(name,sname))
                        continue;
                        printf("id %d name %s surname %s city %s\n",id,name,surname,city);
                }
            }
        CALL_OR_DIE(BF_UnpinBlock(block));
      }
    BF_Block_Destroy(&block);
    return count;
}


int hash(char* name, int buckets){
  int l = 31;
  long int number = 0;
  
  for (int i = 0 ; i < strlen(name) ; i++){    
    number += l*number+(int)name[i];
  }
  int result = number % buckets + 1;
  return result;
}
