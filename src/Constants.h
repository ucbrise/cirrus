#ifndef _CONSTANTS_H_
#define _CONSTANTS_H_

enum PS_OP {
  SEND_LR_GRADIENT,     // 0
  SEND_MF_GRADIENT,     // 1
  GET_LR_FULL_MODEL,    // 2
  GET_MF_FULL_MODEL,    // 3
  GET_LR_SPARSE_MODEL,  // 4
  GET_MF_SPARSE_MODEL,  // 5
  SET_TASK_STATUS,      // 6
  GET_TASK_STATUS,      // 7
  GET_NUM_CONNS,        // 8
  GET_LAST_TIME_ERROR,  // 9
  GET_ALL_TIME_ERROR    // 10
};

#define MAGIC_NUMBER (0x1337)

#endif // _CONSTANTS_H_
