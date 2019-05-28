#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mpi.h>
#include <map>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;
pthread_spinlock_t spinlock;


//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){

  printf("entre a verfic\n");

  MPI_Status sttsaux;

  //TODO: Enviar mensaje TAG_CHAIN_HASH

  MPI_Send(rBlock->block_hash,256,MPI_CHAR,rBlock->node_owner_number,TAG_CHAIN_HASH,MPI_COMM_WORLD);

  printf("mande en verfic\n");

  Block *blockchain = new Block[VALIDATION_BLOCKS];

  //TODO: Recibir mensaje TAG_CHAIN_RESPONSE
  MPI_Recv(blockchain,VALIDATION_BLOCKS,*MPI_BLOCK,rBlock->node_owner_number,TAG_CHAIN_RESPONSE,MPI_COMM_WORLD,&sttsaux);

  //TODO: Verificar que los bloques recibidos
  //sean válidos y se puedan acoplar a la cadena
  printf("recibi en verific\n");

  bool validate_msg = true;
  string aux_result;

  printf("validate: %d\n",validate_msg);

  validate_msg &= string(blockchain[0].block_hash) == string(rBlock->block_hash);
  printf("validate: %d\n",validate_msg);//PREGUNTAR

  validate_msg &= blockchain[0].index == rBlock->index;
  printf("validate: %d\n",validate_msg);

  block_to_hash(rBlock,aux_result);
  validate_msg &= string(blockchain[0].block_hash) == aux_result;
  


  int i = 1;
  for (;i<VALIDATION_BLOCKS;i++){
    if(blockchain[i-1].index == 1){
      break;
    }
    validate_msg &= string(blockchain[i-1].previous_block_hash) == string(blockchain[i].block_hash);
    validate_msg &= blockchain[i].index + 1 == blockchain[i-1].index;
    if(!validate_msg){
      printf("i = %d , index i : %d, index i-1: %d\n",i,blockchain[i].index,blockchain[i-1].index + 1);
    }
  }

  printf("validate: %d\n",validate_msg);
  int received_blocks = i;


  if(validate_msg){

      printf("msg valido\n");

      //TODO: si dentro de los bloques recibidos, alguno ya estaba adentro de nodeblocks(o el ultimo tiene indice 1),
      // entonces ya puedo reconstruir la cadena. agrego todos y actualizo lastblockinchain
      i = 0;

      while (i<received_blocks){
        if(node_blocks.count(blockchain[i].block_hash) == 1 or blockchain[i].index == 1){
          node_blocks[string(blockchain[0].block_hash)]=blockchain[0];
          *last_block_in_chain = blockchain[0];
          
          for(int j = 0 ; j<i ; j++){
            node_blocks[string(blockchain[j].block_hash)]=blockchain[j];

          }
          printf("termino bien verificar\n");

          delete []blockchain;
          return true;
        }
        i++;
      }
  
  
  }

  //si el msg esta mal o no encontre el bloque que buscaba descarto todo y devuelvo false  

  printf("termino mal verificar\n");

  delete []blockchain;
  return false;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status){
  if(valid_new_block(rBlock)){

    //Agrego el bloque al diccionario, aunque no
    //necesariamente eso lo agrega a la cadena
    node_blocks[string(rBlock->block_hash)]=*rBlock;

    //TODO: Si el índice del bloque recibido es 1
    //y mí último bloque actual tiene índice 0,
    //entonces lo agrego como nuevo último.
    if(rBlock->index == 1 and last_block_in_chain->index == 0){

      // node_blocks[string(rBlock->block_hash)]=*rBlock;
      *last_block_in_chain = *rBlock;
      // *last_block_in_chain->block_hash = *rBlock->block_hash;

      // printf("[%d] rblock hash : %s , lastblock hash : %s",mpi_rank,rBlock->block_hash,last_block_in_chain->block_hash);
      // printf("[%d] rblock index : %d , lastblock index : %d",mpi_rank,rBlock->index,last_block_in_chain->index);


      printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
      return true;
    }

    //TODO: Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //y el bloque anterior apuntado por el recibido es mí último actual,
    //entonces lo agrego como nuevo último.
    if(rBlock->index == last_block_in_chain->index + 1 and string(rBlock->previous_block_hash) == string(last_block_in_chain->block_hash)){
      *last_block_in_chain = *rBlock;
      // printf("[%d] rblock hash : %d , lastblock hash : %d",mpi_rank,rBlock->block_hash,last_block_in_chain->block_hash);     
      // printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
      return true;
    }

    //TODO: Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //pero el bloque anterior apuntado por el recibido no es mí último actual,
    //entonces hay una blockchain más larga que la mía.
    if(rBlock->index == last_block_in_chain->index + 1 and string(rBlock->previous_block_hash) != string(last_block_in_chain->block_hash)){
      printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
      bool res = verificar_y_migrar_cadena(rBlock,status);
      return res;
    }


    //TODO: Si el índice del bloque recibido es igua al índice de mi último bloque actual,
    //entonces hay dos posibles forks de la blockchain pero mantengo la mía
    if(rBlock->index == last_block_in_chain->index){
      printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n",mpi_rank,rBlock->index,status->MPI_SOURCE);
      return false;
    }

    //TODO: Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
    //entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
    if(rBlock->index < last_block_in_chain->index ){
      printf("[%d] Conflicto suave: Descarto el bloque (%d vs %d) contra %d \n",mpi_rank,rBlock->index,last_block_in_chain->index, status->MPI_SOURCE);
      return false;
    }

    //TODO: Si el índice del bloque recibido está más de una posición adelantada a mi último bloque actual,
    //entonces me conviene abandonar mi blockchain actual
    if(rBlock->index > last_block_in_chain->index + 1){
      printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
      bool res = verificar_y_migrar_cadena(rBlock,status);
      return res;
    }
    printf("[%d] nada tiene sentido \n",mpi_rank);
    printf("[%d] mi rBlock index es: %d y el last block in chain index es: %d",mpi_rank, rBlock->index,last_block_in_chain->index);
  }

  printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);
  return false;
}


//Envia el bloque minado a todos los nodos
void broadcast_block(const Block *block){
  //No enviar a mí mismo
  //TODO: Completar
  MPI_Request req;
  int i = mpi_rank-1, j = mpi_rank+1;
  while(i>=0 || j<total_nodes){
    if(i>=0){
      MPI_Send(block,1,*MPI_BLOCK,i,TAG_NEW_BLOCK,MPI_COMM_WORLD);
      // MPI_Isend(block,1,*MPI_BLOCK,i,TAG_NEW_BLOCK,MPI_COMM_WORLD,&req);
      i--;
    }
    if(j<total_nodes){
      MPI_Send(block,1,*MPI_BLOCK,j,TAG_NEW_BLOCK,MPI_COMM_WORLD);
      // MPI_Isend(block,1,*MPI_BLOCK,j,TAG_NEW_BLOCK,MPI_COMM_WORLD,&req);
      j++;
    }
  }
}

//Proof of work
//TODO: Advertencia: puede tener condiciones de carrera
void* proof_of_work(void *ptr){
    string hash_hex_str;
    Block block;
    unsigned int mined_blocks = 0;
    while(true){

      block = *last_block_in_chain;

      //Preparar nuevo bloque
      block.index += 1;
      block.node_owner_number = mpi_rank;
      block.difficulty = DEFAULT_DIFFICULTY;
      block.created_at = static_cast<unsigned long int> (time(NULL));
      memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);

      //Agregar un nonce al azar al bloque para intentar resolver el problema
      gen_random_nonce(block.nonce);

      //Hashear el contenido (con el nuevo nonce)
      block_to_hash(&block,hash_hex_str);

      //Contar la cantidad de ceros iniciales (con el nuevo nonce)
      if(solves_problem(hash_hex_str)){

        //mutexlock // PREGUNTAR
          pthread_spin_lock(&spinlock);

          //Verifico que no haya cambiado mientras calculaba
          if(last_block_in_chain->index < block.index){
            mined_blocks += 1;
            *last_block_in_chain = block;
            strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
            node_blocks[hash_hex_str] = *last_block_in_chain;
            printf("[%d] Agregué un producido con index %d \n",mpi_rank,last_block_in_chain->index);

            //TODO: Mientras comunico, no responder mensajes de nuevos nodos
            broadcast_block(last_block_in_chain);
          }
          pthread_spin_unlock(&spinlock);        
        //mutexunlock
      }

    }

    return NULL;
}



int node(){

  //Tomar valor de mpi_rank y de nodos totales
  MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  pthread_spin_init(&spinlock,0);

  //La semilla de las funciones aleatorias depende del mpi_ranking
  srand(time(NULL) + mpi_rank);
  printf("[MPI] Lanzando proceso %u\n", mpi_rank);

  last_block_in_chain = new Block;

  //Inicializo el primer bloque
  last_block_in_chain->index = 0;
  last_block_in_chain->node_owner_number = mpi_rank;
  last_block_in_chain->difficulty = DEFAULT_DIFFICULTY;
  last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
  memset(last_block_in_chain->previous_block_hash,0,HASH_SIZE);

  //TODO: Crear thread para minar

  pthread_t thread;

  pthread_create(&thread, NULL, &proof_of_work,NULL);

      //TODO: Recibir mensajes de otros nodos
  while(true){

    int flg = 0;
    MPI_Status sttsaux;
      //TODO: Si es un mensaje de nuevo bloque, llamar a la función
      // validate_block_for_chain con el bloque recibido y el estado de MPI
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_NEW_BLOCK,MPI_COMM_WORLD, &flg, &sttsaux);
    if(flg){
      Block *buf = new Block;
      MPI_Status* stts = new MPI_Status;
      pthread_spin_lock(&spinlock);
      MPI_Recv(buf, 1, *MPI_BLOCK, MPI_ANY_SOURCE, TAG_NEW_BLOCK,MPI_COMM_WORLD, stts);
      validate_block_for_chain(buf, stts);
      pthread_spin_unlock(&spinlock);

      delete buf;
      delete stts;
    }
      //TODO: Si es un mensaje de pedido de cadena,
      //responderlo enviando los bloques correspondientes
    flg = 0;
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_CHAIN_HASH,MPI_COMM_WORLD, &flg, &sttsaux);
    if(flg){
      flg = 0;
      int src = sttsaux.MPI_SOURCE;
      char buf [256];
      MPI_Status* stts = new MPI_Status;
      MPI_Recv(buf, 256, MPI_CHAR, src, TAG_CHAIN_HASH,MPI_COMM_WORLD, stts);
      delete stts;
      printf("[%d] hola1\n",mpi_rank);

      Block sending_blocks [VALIDATION_BLOCKS];
      Block *rBlock = last_block_in_chain;
      printf("[%d] hola2\n",mpi_rank);

      printf("%d\n",buf);


      printf("%d\n",rBlock->block_hash);
      // printf("%d\n",node_blocks[rBlock->previous_block_hash].previous_block_hash);


      while(rBlock->index !=0){
        printf("[%d] mi index es : %d\n",mpi_rank,rBlock->index);
        if(string(rBlock->block_hash) == string(buf)){
          printf("victory\n");
          break;
        }
        rBlock = &(node_blocks[string(rBlock->previous_block_hash)]);
      }
      for(int i = 0; i<VALIDATION_BLOCKS ; i++){
        sending_blocks[i] = *rBlock;
        if(rBlock->index == 1){break;}
        rBlock=&node_blocks[string(rBlock->previous_block_hash)];
      }


      // printf("[%d] hola3\n",mpi_rank);
      printf("[%d] hash recibido : %d\n",mpi_rank,string(rBlock->block_hash));
      printf("[%d] primer hash mandando : %d\n",mpi_rank,string(sending_blocks[0].block_hash));

      MPI_Send(&sending_blocks,VALIDATION_BLOCKS,*MPI_BLOCK,src,TAG_CHAIN_RESPONSE,MPI_COMM_WORLD);
      printf("[%d] hola4\n",mpi_rank);

    }
  }


 	pthread_join(thread, NULL);

  delete last_block_in_chain;
  return 0;
}