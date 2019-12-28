#ifndef DECLS_H_
#define DECLS_H_

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
        TypeName(const TypeName&) = delete;      \
          void operator=(const TypeName&) = delete

#endif  // DECLS_H_
