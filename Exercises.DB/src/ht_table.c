#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
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


int HT_CreateFile(char *fileName,  int buckets){
    int fd;
    BF_Block* block;
    char* data=NULL;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_CreateFile(fileName));
    CALL_OR_DIE(BF_OpenFile(fileName, &fd));
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
        int recordsnum=0;
        memcpy(data, &next_block_id, sizeof(int));
        data+=sizeof(int);
        memcpy(data,&recordsnum,sizeof(int));
        data+=sizeof(int);
        memcpy(data,&i,sizeof(int));
        int blocknum;
        
        BF_Block_SetDirty(block);
        CALL_OR_DIE(BF_UnpinBlock(block));
    }
     BF_Block_Destroy(&block);
    CALL_OR_DIE(BF_CloseFile(fd));
    return 0;
}

HT_info* HT_OpenFile(char *fileName){
    HT_info *info;
    int fd;
    BF_Block *block;
    char *data;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_OpenFile(fileName, &fd));
    info = malloc(sizeof(HT_info));
    info->fileDesc = fd;
    CALL_OR_DIE(BF_GetBlock(fd, 0, block));
    data = BF_Block_GetData(block);
    memcpy(&info->buckets, data, sizeof(int));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    return info;
}


int HT_CloseFile( HT_info* HT_info ){
    int fd;
    if (HT_info) {
        fd = HT_info->fileDesc;
        free(HT_info);
        CALL_OR_DIE(BF_CloseFile(fd));

        return 0;
    }
    return -1;
}


int HT_InsertEntry(HT_info* ht_info, Record record) {
  int fd, buckets, bucket_id, block_id;
  BF_Block *block;
  char *data;
  char *data2;
  int next_block_id;
  BF_Block_Init(&block);
  fd = ht_info->fileDesc;
  buckets = ht_info->buckets;
  bucket_id = record.id % buckets;
  // Find the block that corresponds to the bucket
  block_id = bucket_id + 1;
  CALL_OR_DIE(BF_GetBlock(fd, block_id, block));
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
  int recordsnum=0;
  data+=sizeof(int);
  int blocknumber;
  memcpy(&recordsnum,data,sizeof(int));
  if (((recordsnum+1)*sizeof(Record) + sizeof(char)+(3*sizeof(int))) < BF_BLOCK_SIZE) {
      // There is space, insert the record and exit the loop
      recordsnum++;
      memcpy(data,&recordsnum,sizeof(int));
      data+=sizeof(int);
      memcpy(&blocknumber,data,sizeof(int));
      data+=sizeof(int) +(recordsnum-1)*sizeof(Record);
      memset(data,0,sizeof(int));
      memcpy(data, &record.id,sizeof(record.id));
      data += sizeof(int);
      memset(data,0,15*sizeof(char));
      memcpy(data , record.name , strlen(record.name)+1);
      data += 15*sizeof(char);
      memset(data,0,20*sizeof(char));
      memcpy(data , record.surname , strlen(record.surname)+1);
      data += 20*sizeof(char);
      memset(data,0,20*sizeof(char));
      memcpy(data , record.city , strlen(record.city)+1); 

      return blocknumber;
  } else {
    // No space
    // Allocate a new block and add it to the end of the bucket
    CALL_OR_DIE(BF_AllocateBlock(fd, block));
    data2 = BF_Block_GetData(block);
    memset(data2, 0, BF_BLOCK_SIZE);
    next_block_id=-1;
    memcpy(data2,&next_block_id,sizeof(int));
    recordsnum =1;
    data2+=sizeof(int);
    memset(data2,0,sizeof(int));
    memcpy(data2, &recordsnum, sizeof(int));
    data2+=sizeof(int);
    memset(data2,0,sizeof(int));
    CALL_OR_DIE(BF_GetBlockCounter(fd,&blocknumber));
    blocknumber--;
    memcpy(data2, &blocknumber, sizeof(int));
    int blocknum;
    memcpy(&blocknum,data2,sizeof(int));
    data2+=sizeof(int);
    memset(data2,0,sizeof(int));
    memcpy(data2, &record.id,sizeof(record.id));
    data2 += sizeof(int);
    memset(data2,0,15*sizeof(char));
    memcpy(data2 , record.name , strlen(record.name)+1);
    data2 += 15*sizeof(char);
    memset(data2,0,20*sizeof(char));
    memcpy(data2 , record.surname , strlen(record.surname)+1);
    data2 += 20*sizeof(char);
    memset(data2,0,20*sizeof(char));
    memcpy(data2 , record.city , strlen(record.city)+1); 

    data-=sizeof(int);
    memset(data,0,sizeof(int));
    memcpy(data,&blocknumber,sizeof(int));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return blocknumber;
  }
  return 0;
}


int HT_GetAllEntries(HT_info* ht_info, void *value )
    {
    int id;
    char name[15];
    char surname[20];
    char city[20];
    int fd, buckets, block_id;
    BF_Block *block;
    char *data;
    int next_block_id;
    Record record;
    int count=0;
    BF_Block_Init(&block);
    if (!ht_info || !value) {
        return -1;
    }
    fd = ht_info->fileDesc;
    buckets = ht_info->buckets;
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
            data+=2*sizeof(int);
            int j;
            for(j=0;j<recordsnum;j++){
              memcpy(&id,data+j*sizeof(Record),sizeof(int));
              memcpy(&name,data+(j*sizeof(Record)+sizeof(int)),15*sizeof(char));
              memcpy(&surname,data+(j*sizeof(Record)+15*sizeof(char)+sizeof(int)),20*sizeof(char));
              memcpy(&city,data+(j*sizeof(Record)+20*sizeof(char)+15*sizeof(char)+sizeof(int)),20*sizeof(char));

              if(id==*(int *)value){
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
        data+=2*sizeof(int);
        int j;

        for(j=0;j<recordsnum;j++){
              
              memcpy(&id,data+j*sizeof(Record),sizeof(int));
              memcpy(&name,data+(j*sizeof(Record)+sizeof(int)),15*sizeof(char));
              memcpy(&surname,data+(j*sizeof(Record)+15*sizeof(char)+sizeof(int)),20*sizeof(char));
              memcpy(&city,data+(j*sizeof(Record)+20*sizeof(char)+15*sizeof(char)+sizeof(int)),20*sizeof(char));

              if(id==*(int *)value){
                  printf("id %d name %s surname %s city %s\n",id,name,surname,city);
              }
            }
        CALL_OR_DIE(BF_UnpinBlock(block));
      }
    BF_Block_Destroy(&block);
    return count;
}




