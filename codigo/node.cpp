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
#include <fstream>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;
pthread_spinlock_t spinlock;
bool mequedeatras = false;


//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){

  MPI_Status sttsaux;

  //TODO: Enviar mensaje TAG_CHAIN_HASH

  MPI_Send(rBlock->block_hash,256,MPI_CHAR,rBlock->node_owner_number,TAG_CHAIN_HASH,MPI_COMM_WORLD);

  Block *blockchain = new Block[VALIDATION_BLOCKS];

  //TODO: Recibir mensaje TAG_CHAIN_RESPONSE
  MPI_Recv(blockchain,VALIDATION_BLOCKS,*MPI_BLOCK,rBlock->node_owner_number,TAG_CHAIN_RESPONSE,MPI_COMM_WORLD,&sttsaux);

  //TODO: Verificar que los bloques recibidos
  //sean válidos y se puedan acoplar a la cadena

  bool validate_msg = true;
  string aux_result;

  //cheque que el msj que me llego sea valido, y veo cuantos bloques me llegaron(si es un indice bajo,
  //puede que sea menos que VALIDATION_BLOCKS

  validate_msg &= string(blockchain[0].block_hash) == string(rBlock->block_hash);
  validate_msg &= blockchain[0].index == rBlock->index;
  block_to_hash(&blockchain[0],aux_result);
  validate_msg &= string(blockchain[0].block_hash) == aux_result;

  int i = 1;
  for (;i<VALIDATION_BLOCKS;i++){
    if(blockchain[i-1].index == 1){
      break;
    }
    validate_msg &= string(blockchain[i-1].previous_block_hash) == string(blockchain[i].block_hash);
    validate_msg &= blockchain[i].index + 1 == blockchain[i-1].index;
    block_to_hash(&blockchain[i],aux_result);
    validate_msg &= string(blockchain[i].block_hash) == aux_result;
  }

  int received_blocks = i;

  if(validate_msg){

      //TODO: si dentro de los bloques recibidos, alguno ya estaba adentro de nodeblocks(o el ultimo tiene indice 1),
      // entonces ya puedo reconstruir la cadena. agrego todos y actualizo lastblockinchain
      i = 1;

      while (i<received_blocks){
        if(node_blocks.count(string(blockchain[i].block_hash)) == 1 or blockchain[i].index == 1){
          node_blocks[string(blockchain[0].block_hash)]=blockchain[0];
          *last_block_in_chain = blockchain[0];
          // printf("[%d] Actualize mi lastbloque, recibi %d bloques\n",mpi_rank,received_blocks);
          
          for(int j = 0 ; j<i ; j++){
            node_blocks[string(blockchain[j].block_hash)]=blockchain[j];

          }
          delete []blockchain;
          return true;
        }
        i++;
      }
  
  
  }

  //si el msg esta mal o no encontre el bloque que buscaba descarto todo y devuelvo false  

  // printf("termino mal verificar\n");

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

      *last_block_in_chain = *rBlock;
      printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
      return true;
    }

    //TODO: Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //y el bloque anterior apuntado por el recibido es mí último actual,
    //entonces lo agrego como nuevo último.
    if(rBlock->index == last_block_in_chain->index + 1 and string(rBlock->previous_block_hash) == string(last_block_in_chain->block_hash)){
      *last_block_in_chain = *rBlock;
      printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,status->MPI_SOURCE);
      return true;
    }

    //TODO: Si el índice del bloque recibido es
    //el siguiente a mí último bloque actual,
    //pero el bloque anterior apuntado por el recibido no es mí último actual,
    //entonces hay una blockchain más larga que la mía.
    if(rBlock->index == last_block_in_chain->index + 1 and string(rBlock->previous_block_hash) != string(last_block_in_chain->block_hash)){
      printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
      bool res;
      res = verificar_y_migrar_cadena(rBlock,status);
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
      bool res;
      res = verificar_y_migrar_cadena(rBlock,status);
      mequedeatras = !res;
      return res;
    }
    printf("[%d] nada tiene sentido \n",mpi_rank);
  }

  printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);
  return false;
}


//Envia el bloque minado a todos los nodos
void broadcast_block(const Block *block){
  //No enviar a mí mismo
  //TODO: Completar
  // MPI_Request req;
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

void broadcast_termination(){
  int buf = 1;
  printf("[%d] Termine\n",mpi_rank);
  int i = mpi_rank-1, j = mpi_rank+1;
  while(i>=0 || j<total_nodes){
    if(i>=0){
      MPI_Send(&buf,1,MPI_INT,i,TAG_TERMINATION,MPI_COMM_WORLD);
      i--;
    }
    if(j<total_nodes){
      MPI_Send(&buf,1,MPI_INT,j,TAG_TERMINATION,MPI_COMM_WORLD);
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

      if(last_block_in_chain->index == MAX_BLOCKS){
        broadcast_termination();
        return NULL;
      }

      if(mequedeatras){
        return NULL;
      }

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

        //lock para que no se agreguen bloques con el mismo indice, si esta llegando un msj de otro nodo,
        //y yo hice un bloque con ese indice, el no voy a entrar al if y el bloque se ignora
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
  printf("[MPI] Lanzando proceso %u de %d\n", mpi_rank,total_nodes);

  last_block_in_chain = new Block;

  //Inicializo el primer bloque
  last_block_in_chain->index = 0;
  last_block_in_chain->node_owner_number = mpi_rank;
  last_block_in_chain->difficulty = DEFAULT_DIFFICULTY;
  last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
  memset(last_block_in_chain->previous_block_hash,0,HASH_SIZE);

  //TODO: Crear thread para minar

  pthread_t thread;
  int terminaron = 1;//empieza en 1 porque no va a recibir un msj que el mismo termino

  pthread_create(&thread, NULL, &proof_of_work,NULL);

      //TODO: Recibir mensajes de otros nodos
  // while(last_block_in_chain->index != MAX_BLOCKS){
  while(terminaron < total_nodes){
    // if(terminaron >0){
    //   printf("[%d] terminaron : %d",mpi_rank,terminaron);
    // }

    int flg = 0;
    MPI_Status sttsaux;
      //TODO: Si es un mensaje de nuevo bloque, llamar a la función
      // validate_block_for_chain con el bloque recibido y el estado de MPI
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_TERMINATION,MPI_COMM_WORLD, &flg, &sttsaux);
    if(flg){
      int buf;
      MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, TAG_TERMINATION,MPI_COMM_WORLD, &sttsaux);
      terminaron += buf;
    }
    flg=0;
    
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

      if(mequedeatras){
        broadcast_termination();
        //aviso que me quede atras, y termino mi ciclo 
        break;
      }
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

      Block sending_blocks [VALIDATION_BLOCKS];
      Block *rBlock = last_block_in_chain;

      while(rBlock->index !=0){
        if(string(rBlock->block_hash) == string(buf)){
          break;
        }
        rBlock = &(node_blocks[string(rBlock->previous_block_hash)]);
      }
      for(int i = 0; i<VALIDATION_BLOCKS ; i++){
        sending_blocks[i] = *rBlock;
        if(rBlock->index == 1){break;}
        rBlock=&node_blocks[string(rBlock->previous_block_hash)];
      }


      MPI_Send(&sending_blocks,VALIDATION_BLOCKS,*MPI_BLOCK,src,TAG_CHAIN_RESPONSE,MPI_COMM_WORLD);

    }
  }


 	pthread_join(thread, NULL);

  printf("[%d] Imprimiendo Log\n",mpi_rank);

  ofstream myfile;
	myfile.open ("log/"+to_string(mpi_rank)+".txt");

  if(mequedeatras){
    myfile << "Me quede atras en :" << endl;
  }

  while(last_block_in_chain->index != 0){
    myfile << last_block_in_chain->index << " " << last_block_in_chain->block_hash << endl;    
    *last_block_in_chain = node_blocks[string(last_block_in_chain->previous_block_hash)];
  }

  myfile.close();

  delete last_block_in_chain;
  return 0;
}