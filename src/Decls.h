#ifndef _DECLS_H_
#define _DECLS_H_

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
        TypeName(const TypeName&) = delete;      \
          void operator=(const TypeName&) = delete

#endif  // _DECLS_H_
