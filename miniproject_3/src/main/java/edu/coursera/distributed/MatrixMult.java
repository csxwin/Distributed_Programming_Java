package edu.coursera.distributed;

import edu.coursera.distributed.util.MPI;
import edu.coursera.distributed.util.MPI.MPIException;

/**
 * A wrapper class for a parallel, MPI-based matrix multiply implementation.
 */
public class MatrixMult {
    /**
     * A parallel implementation of matrix multiply using MPI to express SPMD
     * parallelism. In particular, this method should store the output of
     * multiplying the matrices a and b into the matrix c.
     *
     * This method is called simultaneously by all MPI ranks in a running MPI
     * program. For simplicity MPI_Init has already been called, and
     * MPI_Finalize should not be called in parallelMatrixMultiply.
     *
     * On entry to parallelMatrixMultiply, the following will be true of a, b,
     * and c:
     *
     *   1) The matrix a will only be filled with the input values on MPI rank
     *      zero. Matrix a on all other ranks will be empty (initialized to all
     *      zeros).
     *   2) Likewise, the matrix b will only be filled with input values on MPI
     *      rank zero. Matrix b on all other ranks will be empty (initialized to
     *      all zeros).
     *   3) Matrix c will be initialized to all zeros on all ranks.
     *
     * Upon returning from parallelMatrixMultiply, the following must be true:
     *
     *   1) On rank zero, matrix c must be filled with the final output of the
     *      full matrix multiplication. The contents of matrix c on all other
     *      ranks are ignored.
     *
     * Therefore, it is the responsibility of this method to distribute the
     * input data in a and b across all MPI ranks for maximal parallelism,
     * perform the matrix multiply in parallel, and finally collect the output
     * data in c from all ranks back to the zeroth rank. You may use any of the
     * MPI APIs provided in the mpi object to accomplish this.
     *
     * A reference sequential implementation is provided below, demonstrating
     * the use of the Matrix class's APIs.
     *
     * @param a Input matrix
     * @param b Input matrix
     * @param c Output matrix
     * @param mpi MPI object supporting MPI APIs
     * @throws MPIException On MPI error. It is not expected that your
     *                      implementation should throw any MPI errors during
     *                      normal operation.
     */
    public static void parallelMatrixMultiply(Matrix a, Matrix b, Matrix c,
            final MPI mpi) throws MPIException {
        // for (int i = 0; i < c.getNRows(); i++) {
        //     for (int j = 0; j < c.getNCols(); j++) {
        //         c.set(i, j, 0.0);

        //         for (int k = 0; k < b.getNRows(); k++) {
        //             c.incr(i, j, a.get(i, k) * b.get(k, j));
        //         }
        //     }
        // }

        int MPI_SIZE = mpi.MPI_Comm_size(mpi.MPI_COMM_WORLD);
        int width = b.getNRows() / MPI_SIZE;
        int rank = mpi.MPI_Comm_rank(mpi.MPI_COMM_WORLD);

        if (rank == 0) {
            for (int n = 1; n < MPI_SIZE; ++n) {
                // todo?
                final Double[] tempA = new Double[width * a.getNRows()];
                int idx = 0;
                for (int row = 0; row < a.getNRows(); ++row) {
                    tempA[idx] = a.get(row, idx % width);
                    ++idx;
                }
                mpi.MPI_Send(tempA, 0, tempA.length, n, 0, mpi.MPI_COMM_WORLD);
                mpi.MPI_Send(b.getValues(), n * b.getNCols() * width, b.getNCols() * width, n, 0, mpi.MPI_COMM_WORLD);
            }
        }
        else {
            for (int r = 0; r < a.getNRows(); ++r) {
                mpi.MPI_Recv(a.getValues(), a.getOffsetOfRow(r) + rank, width, 0, 0, mpi.MPI_COMM_WORLD);
            }
            mpi.MPI_Recv(b.getValues(), rank * b.getNCols() * width, b.getNCols() * width, 0, 0, mpi.MPI_COMM_WORLD);
        }

        for (int i = 0; i < c.getNRows() / (double)MPI_SIZE  ; i++) {
            for (int j = 0; j < c.getNCols(); j++) {
                c.set(i, j, 0.0);
                for (int k = rank * width; k < (rank + 1) * width; k++) {
                    c.incr(i, j, a.get(i, k) * b.get(k, j));
                }
            }
        }

        if (rank != 0) {
            mpi.MPI_Send(c.getValues(), 0, c.getValues().length, 0, 0, mpi.MPI_COMM_WORLD);
        }
        else {
            for (int r = 0; r < MPI_SIZE; ++r) {
                final Double[] tempC = new Double[c.getNCols() * c.getNRows()];
                mpi.MPI_Recv(tempC, 0, tempC.length, r, 0, mpi.MPI_COMM_WORLD);
                for (int i = 0; i < c.getNRows() / (double)MPI_SIZE  ; i++) {
                    for (int j = 0; j < c.getNCols(); j++) {
                        c.incr(i, j, tempC[i * c.getNCols() + j]);
                    }
                }
            }
        }
    }
}
