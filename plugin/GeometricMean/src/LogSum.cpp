#include "CoreConcept.h"
#include "Exceptions.h"
#include "ScalarImp.h"

#include "LogSum.h"

ConstantSP logSum(const ConstantSP &x, const ConstantSP &placeholder) {
    string syntax = "Usage: logSum::logSum(x). ";
    if (!x->isScalar() && !x->isVector())
        throw IllegalArgumentException("logSum::logSum", syntax + "x must be a number or a numeric vector.");
    switch (x->getType()) {
        case DT_CHAR: return computeLogSum<char>(x);
        case DT_SHORT: return computeLogSum<short>(x);
        case DT_INT: return computeLogSum<int>(x);
        case DT_LONG: return computeLogSum<long long>(x);
        case DT_DOUBLE: return computeLogSum<double>(x);
        case DT_FLOAT: return computeLogSum<float>(x);
        default: throw IllegalArgumentException("logSum::logSum", syntax + "x must be a number or a numeric vector.");
    }
}