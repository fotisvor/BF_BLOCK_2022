#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h" 
#include "hp_file.h"
#include "record.h"

#define CALL_BF(call)   
#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }



int HP_CreateFile(char *fileName){
    // Create and open a new file

    CALL_OR_DIE(BF_CreateFile(fileName));
    int fileDesc;
    CALL_OR_DIE(BF_OpenFile(fileName,&fileDesc));
    char* data=NULL;

    BF_Block* block = NULL;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_AllocateBlock(fileDesc,block));  
    data = BF_Block_GetData(block);
    int blocksnum=0;
    int recordsnum= 0;

    memset(data,0,BF_BLOCK_SIZE);
    memcpy(data,&blocksnum,sizeof(blocksnum));
    memcpy(data+(sizeof(blocksnum)),&recordsnum,sizeof(recordsnum));
    
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    CALL_OR_DIE(BF_CloseFile(fileDesc));


    return 0;
}

HP_info* HP_OpenFile(char *filename){
    int file_desc;
    HP_info* hp_info = malloc(sizeof(HP_info));
    CALL_OR_DIE(BF_OpenFile(filename,&file_desc));

    hp_info->fileDesc=file_desc;
    return hp_info;  
}


int HP_CloseFile( HP_info* hp_info ){
    CALL_BF(BF_CloseFile(hp_info->fileDesc));
}

int HP_InsertEntry(HP_info* hp_info, Record record){

    BF_Block* block = NULL;
    char* data = NULL;
    int block_num = 0;

    //get number of blocks

    CALL_OR_DIE(BF_GetBlockCounter(hp_info->fileDesc,&block_num));
    if(block_num == 1 ){
        //create first block
        BF_Block_Init(&block);
        CALL_OR_DIE(BF_AllocateBlock(hp_info->fileDesc,block));
        data = BF_Block_GetData(block);
        memset(data,(int)0,BF_BLOCK_SIZE);
        int records = 1;
        //number of records in the block is one
        memcpy(data,&records,sizeof(int));
        data += sizeof(int);

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
    }
    else{
        block_num--;
        BF_Block_Init(&block);
        CALL_OR_DIE(BF_GetBlock(hp_info->fileDesc,block_num,block));
        data = BF_Block_GetData(block);
        int record_num = 0;
        memcpy(&record_num,data,sizeof(int));
        //if there is no space
        if(((record_num+1)*sizeof(Record) +sizeof(int) + sizeof(char)) >= BF_BLOCK_SIZE){
            //Unpin previous block
            CALL_OR_DIE(BF_UnpinBlock(block));
            BF_Block_Destroy(&block);
            //Allocate new block
            BF_Block_Init(&block);
            CALL_OR_DIE(BF_AllocateBlock(hp_info->fileDesc,block));
            data = BF_Block_GetData(block);
            memset(data,0,BF_BLOCK_SIZE);

            int records = 1;
            memcpy(data,&records,sizeof(int));
            data += sizeof(int);
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
            block_num--; 
        } 
        else{//There is space
            int new_rec = ++record_num;
            memcpy(data,&new_rec,sizeof(int));
            data += sizeof(int) + (new_rec-1)*sizeof(Record);
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
            data-=sizeof(int) + 15*sizeof(char) + 20*sizeof(char) + 20*sizeof(char);
            char* buffer = data + sizeof(int); 

        } 
    }
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    
}


int HP_GetAllEntries(HP_info* hp_info, int value){
    BF_Block* block;
    BF_Block_Init(&block);
    int i ;
    int block_num ;
    CALL_OR_DIE(BF_GetBlockCounter(hp_info->fileDesc,&block_num));
    int records;
    for(i = 1 ; i < block_num; i++){
        CALL_OR_DIE(BF_GetBlock(hp_info->fileDesc,i,block));
        char*data = BF_Block_GetData(block);
        int id;
        char name[15];
        char surname[20];
        char city[20];
        

        memcpy(&records,data,sizeof(int));
        
        data += sizeof(int); 
        for(int j = 0 ; j < records; j++){
            
            memcpy(&id,data+j*sizeof(Record),sizeof(int));
            memcpy(&name,data+(j*sizeof(Record)+sizeof(int)),15*sizeof(char));
            memcpy(&surname,data+(j*sizeof(Record)+15*sizeof(char)+sizeof(int)),20*sizeof(char));
            memcpy(&city,data+(j*sizeof(Record)+20*sizeof(char)+15*sizeof(char)+sizeof(int)),20*sizeof(char));
           if(id==value){
                printf("id %d name %s surname %s city %s\n",id,name,surname,city);
            }
            
            
        }
        CALL_OR_DIE(BF_UnpinBlock(block));
    }

    BF_Block_Destroy(&block);
    return 0;
}

