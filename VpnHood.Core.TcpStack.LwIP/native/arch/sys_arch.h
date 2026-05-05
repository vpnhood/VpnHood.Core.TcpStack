/*
 * arch/sys_arch.h - System architecture definitions for NO_SYS mode.
 * In NO_SYS=1 mode, most sys_arch types are unused but must be defined.
 */
#ifndef LWIP_ARCH_SYS_ARCH_H
#define LWIP_ARCH_SYS_ARCH_H

/* In NO_SYS mode, these types are not used but must be defined */
typedef int sys_sem_t;
typedef int sys_mutex_t;
typedef int sys_mbox_t;
typedef int sys_thread_t;

#define sys_sem_valid(sem)       0
#define sys_sem_set_invalid(sem)
#define sys_mutex_valid(mutex)   0
#define sys_mutex_set_invalid(mutex)
#define sys_mbox_valid(mbox)     0
#define sys_mbox_set_invalid(mbox)
#define SYS_MBOX_NULL            0

#endif /* LWIP_ARCH_SYS_ARCH_H */
