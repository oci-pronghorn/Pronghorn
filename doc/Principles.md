
# Pronghorn Stage Creation Principles

## Pronghorn is *not* Memory efficient
## Pronghorn _is_ CPU efficient

### Principles
- Never block in run()

- new() is only done in startup().   Please note that the String catenate operator, +, causes new()

- no boxed primitives

- short, as in <= 7 lines, methods

- Do use either while(true) , for (;;)
 
- Do *not* use iterators or the enhanced for, i.e., for (element : collection)

- Invariant removal


### Simple != Easy.
