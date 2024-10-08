/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <base/arithmeticOverflow.h>

namespace local_engine
{

static bool canCastLower(const Int256 & a, const Int256 & b)
{
    if (a.items[2] == 0 && a.items[3] == 0 && b.items[2] == 0 && b.items[3] == 0)
        return true;

    return false;
}

static bool canCastLower(const Int128 & a, const Int128 & b)
{
    if (a.items[1] == 0 && b.items[1] == 0)
        return true;

    return false;
}

struct DecimalPlusImpl
{
    template <typename A>
    static bool apply(A & a, A & b, A & r)
    {
        return !common::addOverflow(a, b, r);
    }

    template <>
    static bool apply(Int128 & a, Int128 & b, Int128 & r)
    {
        if (canCastLower(a, b))
        {
            UInt64 low_result;
            if (common::addOverflow(static_cast<UInt64>(a), static_cast<UInt64>(b), low_result))
                return !common::addOverflow(a, b, r);

            r = static_cast<Int128>(low_result);
            return true;
        }

        return !common::addOverflow(a, b, r);
    }


    template <>
    static bool apply(Int256 & a, Int256 & b, Int256 & r)
    {
        if (canCastLower(a, b))
        {
            UInt128 low_result;
            if (common::addOverflow(static_cast<UInt128>(a), static_cast<UInt128>(b), low_result))
                return !common::addOverflow(a, b, r);

            r = static_cast<Int256>(low_result);
            return true;
        }

        return !common::addOverflow(a, b, r);
    }


#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateAdd(left, right) : b.CreateFAdd(left, right);
    }
#endif
};

struct DecimalMinusImpl
{
    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename A>
    static bool apply(A & a, A & b, A & r)
    {
        return !common::subOverflow(a, b, r);
    }

    template <>
    static bool apply(Int128 & a, Int128 & b, Int128 & r)
    {
        if (canCastLower(a, b))
        {
            UInt64 low_result;
            if (common::subOverflow(static_cast<UInt64>(a), static_cast<UInt64>(b), low_result))
                return !common::subOverflow(a, b, r);

            r = static_cast<Int128>(low_result);
            return true;
        }

        return !common::subOverflow(a, b, r);
    }

    template <>
    static bool apply(Int256 & a, Int256 & b, Int256 & r)
    {
        if (canCastLower(a, b))
        {
            UInt128 low_result;
            if (common::subOverflow(static_cast<UInt128>(a), static_cast<UInt128>(b), low_result))
                return !common::subOverflow(a, b, r);

            r = static_cast<Int256>(low_result);
            return true;
        }

        return !common::subOverflow(a, b, r);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};


struct DecimalMultiplyImpl
{
    /// Apply operation and check overflow. It's used for Decimal operations. @returns true if overflowed, false otherwise.
    template <typename A>
    static bool apply(A & a, A & b, A & c)
    {
        return !common::mulOverflow(a, b, c);
    }

    template <Int128>
    static bool apply(Int128 & a, Int128 & b, Int128 & r)
    {
        if (canCastLower(a, b))
        {
            UInt64 low_result = 0;
            if (common::mulOverflow(static_cast<UInt64>(a), static_cast<UInt64>(b), low_result))
                return !common::mulOverflow(a, b, r);

            r = static_cast<Int128>(low_result);
            return true;
        }

        return !common::mulOverflow(a, b, r);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateMul(left, right) : b.CreateFMul(left, right);
    }
#endif
};

struct DecimalDivideImpl
{
    template <typename A>
    static bool apply(A & a, A & b, A & r)
    {
        if (b == 0)
            return false;

        r = a / b;
        return true;
    }

    template <>
    static bool apply(Int128 & a, Int128 & b, Int128 & r)
    {
        if (b == 0)
            return false;

        if (canCastLower(a, b))
        {
            r = static_cast<Int128>(static_cast<UInt64>(a) / static_cast<UInt64>(b));
            return true;
        }

        r = a / b;
        return true;
    }

    template <>
    static bool apply(Int256 & a, Int256 & b, Int256 & r)
    {
        if (b == 0)
            return false;

        if (canCastLower(a, b))
        {
            UInt128 low_result = 0;
            UInt128 low_a = static_cast<UInt128>(a);
            UInt128 low_b = static_cast<UInt128>(b);
            apply(low_a, low_b, low_result);
            r = static_cast<Int256>(low_result);
            return true;
        }

        r = a / b;
        return true;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};


// ModuloImpl
struct DecimalModuloImpl
{
    template <typename A>
    static bool apply(A & a, A & b, A & r)
    {
        if (b == 0)
            return false;

        r = a % b;
        return true;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * left, llvm::Value * right, bool)
    {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right) : b.CreateFSub(left, right);
    }
#endif
};

template <typename Op1, typename Op2>
struct IsSameOperation
{
    static constexpr bool value = std::is_same_v<Op1, Op2>;
};

template <typename Op>
struct SparkIsOperation
{
    static constexpr bool plus = IsSameOperation<Op, DecimalPlusImpl>::value;
    static constexpr bool minus = IsSameOperation<Op, DecimalMinusImpl>::value;
    static constexpr bool multiply = IsSameOperation<Op, DecimalMultiplyImpl>::value;
    static constexpr bool division = IsSameOperation<Op, DecimalDivideImpl>::value;
    static constexpr bool modulo = IsSameOperation<Op, DecimalModuloImpl>::value;
};
}
