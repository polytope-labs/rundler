// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Test} from "forge-std/Test.sol";

import "@account-abstraction/interfaces/PackedUserOperation.sol";

import {VerificationGasEstimationHelper} from "../src/VerificationGasEstimationHelper.sol";

contract VerificationGasEstimationHelperTest is Test {
    address constant PAYMASTER = address(0xBEEF);

    Harness harness;

    function setUp() public {
        harness = new Harness();
    }

    function test_setPaymasterVerificationGasKeepsPostOpGasLimit() public view {
        PackedUserOperation memory userOp = _userOp(
            abi.encodePacked(PAYMASTER, uint128(0), uint128(50_000))
        );

        userOp = harness.setPaymasterVerificationGas(userOp, 120_000);

        assertEq(
            userOp.paymasterAndData,
            abi.encodePacked(PAYMASTER, uint128(120_000), uint128(50_000))
        );
    }

    function testFuzz_setPaymasterVerificationGasOnlyWritesItsField(
        uint128 verificationGasLimit,
        uint128 postOpGasLimit,
        uint128 gas,
        bytes memory paymasterData
    ) public view {
        PackedUserOperation memory userOp = _userOp(
            abi.encodePacked(
                PAYMASTER,
                verificationGasLimit,
                postOpGasLimit,
                paymasterData
            )
        );

        userOp = harness.setPaymasterVerificationGas(userOp, gas);

        assertEq(
            userOp.paymasterAndData,
            abi.encodePacked(PAYMASTER, gas, postOpGasLimit, paymasterData)
        );
    }

    function _userOp(
        bytes memory paymasterAndData
    ) internal pure returns (PackedUserOperation memory userOp) {
        userOp.accountGasLimits = bytes32(
            (uint256(100_000) << 128) | 200_000
        );
        userOp.paymasterAndData = paymasterAndData;
    }
}

contract Harness is VerificationGasEstimationHelper {
    function setPaymasterVerificationGas(
        PackedUserOperation memory userOp,
        uint256 gas
    ) external pure returns (PackedUserOperation memory) {
        _setPaymasterVerificationGas(userOp, gas, 0);
        return userOp;
    }
}
